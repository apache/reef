/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.examples.group.bgd;

import org.apache.reef.examples.group.bgd.data.Example;
import org.apache.reef.examples.group.bgd.loss.LossFunction;
import org.apache.reef.examples.group.bgd.operatornames.*;
import org.apache.reef.examples.group.bgd.parameters.AllCommunicationGroup;
import org.apache.reef.examples.group.bgd.parameters.ProbabilityOfFailure;
import org.apache.reef.examples.group.bgd.utils.StepSizes;
import org.apache.reef.examples.group.utils.math.DenseVector;
import org.apache.reef.examples.group.utils.math.Vector;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

public class SlaveTask implements Task {

  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private final double FAILURE_PROB;

  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Receiver<Pair<Vector, Vector>> modelAndDescentDirectionBroadcaster;
  private final Broadcast.Receiver<Vector> descentDirectionBroadcaster;
  private final Reduce.Sender<Pair<Vector, Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Receiver<Double> minEtaBroadcaster;
  private List<Example> examples = null;
  private final ExampleList dataSet;
  private final LossFunction lossFunction;
  private final StepSizes ts;

  private Vector model = null;
  private Vector descentDirection = null;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient,
      final ExampleList dataSet,
      final LossFunction lossFunction,
      @Parameter(ProbabilityOfFailure.class) final double pFailure,
      final StepSizes ts) {

    this.dataSet = dataSet;
    this.lossFunction = lossFunction;
    this.FAILURE_PROB = pFailure;
    LOG.info("Using pFailure=" + this.FAILURE_PROB);
    this.ts = ts;

    this.communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroup.getBroadcastReceiver(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroup.getReduceSender(LossAndGradientReducer.class);
    this.modelAndDescentDirectionBroadcaster = communicationGroup.getBroadcastReceiver(ModelAndDescentDirectionBroadcaster.class);
    this.descentDirectionBroadcaster = communicationGroup.getBroadcastReceiver(DescentDirectionBroadcaster.class);
    this.lineSearchEvaluationsReducer = communicationGroup.getReduceSender(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroup.getBroadcastReceiver(MinEtaBroadcaster.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    /*
     * In the case where there will be evaluator failure and data is not in
     * memory we want to load the data while waiting to join the communication
     * group
     */
    loadData();

    for (boolean repeat = true; repeat; ) {

      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch (controlMessage) {

        case Stop:
          repeat = false;
          break;

        case ComputeGradientWithModel:
          failPerhaps();
          this.model = modelBroadcaster.receive();
          lossAndGradientReducer.send(computeLossAndGradient());
          break;

        case ComputeGradientWithMinEta:
          failPerhaps();
          final double minEta = minEtaBroadcaster.receive();
          assert (descentDirection != null);
          this.descentDirection.scale(minEta);
          assert (model != null);
          this.model.add(descentDirection);
          lossAndGradientReducer.send(computeLossAndGradient());
          break;

        case DoLineSearch:
          failPerhaps();
          this.descentDirection = descentDirectionBroadcaster.receive();
          lineSearchEvaluationsReducer.send(lineSearchEvals());
          break;

        case DoLineSearchWithModel:
          failPerhaps();
          final Pair<Vector, Vector> modelAndDescentDir = modelAndDescentDirectionBroadcaster.receive();
          this.model = modelAndDescentDir.first;
          this.descentDirection = modelAndDescentDir.second;
          lineSearchEvaluationsReducer.send(lineSearchEvals());
          break;

        default:
          break;
      }
    }

    return null;
  }

  private void failPerhaps() {
    if (Math.random() < FAILURE_PROB) {
      throw new RuntimeException("Simulated Failure");
    }
  }

  private Pair<Vector, Integer> lineSearchEvals() {

    if (examples == null) {
      loadData();
    }

    final Vector zed = new DenseVector(examples.size());
    final Vector ee = new DenseVector(examples.size());

    for (int i = 0; i < examples.size(); i++) {
      final Example example = examples.get(i);
      double f = example.predict(model);
      zed.set(i, f);
      f = example.predict(descentDirection);
      ee.set(i, f);
    }

    final double[] t = ts.getT();
    final Vector evaluations = new DenseVector(t.length);
    int i = 0;
    for (final double d : t) {
      double loss = 0;
      for (int j = 0; j < examples.size(); j++) {
        final Example example = examples.get(j);
        final double val = zed.get(j) + d * ee.get(j);
        loss += this.lossFunction.computeLoss(example.getLabel(), val);
      }
      evaluations.set(i++, loss);
    }

    return new Pair<>(evaluations, examples.size());
  }

  private Pair<Pair<Double, Integer>, Vector> computeLossAndGradient() {

    if (examples == null) {
      loadData();
    }

    final Vector gradient = new DenseVector(model.size());
    double loss = 0.0;
    for (final Example example : examples) {
      final double f = example.predict(model);
      final double g = this.lossFunction.computeGradient(example.getLabel(), f);
      example.addGradient(gradient, g);
      loss += this.lossFunction.computeLoss(example.getLabel(), f);
    }

    return new Pair<>(new Pair<>(loss, examples.size()), gradient);
  }

  private void loadData() {
    LOG.info("Loading data");
    examples = dataSet.getExamples();
  }
}
