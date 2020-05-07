/*
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

import org.apache.reef.examples.group.bgd.operatornames.*;
import org.apache.reef.examples.group.bgd.parameters.*;
import org.apache.reef.examples.group.bgd.utils.StepSizes;
import org.apache.reef.examples.group.utils.math.DenseVector;
import org.apache.reef.examples.group.utils.math.Vector;
import org.apache.reef.examples.group.utils.timer.Timer;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.GroupChanges;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master task for BGD example.
 */
public class MasterTask implements Task {

  public static final String TASK_ID = "MasterTask";

  private static final Logger LOG = Logger.getLogger(MasterTask.class.getName());

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Sender<Pair<Vector, Vector>> modelAndDescentDirectionBroadcaster;
  private final Broadcast.Sender<Vector> descentDriectionBroadcaster;
  private final Reduce.Receiver<Pair<Vector, Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Sender<Double> minEtaBroadcaster;
  private final boolean ignoreAndContinue;
  private final StepSizes ts;
  private final double lambda;
  private final int maxIters;
  private final ArrayList<Double> losses = new ArrayList<>();
  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<>();
  private final Vector model;

  private boolean sendModel = true;
  private double minEta = 0;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      @Parameter(ModelDimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Iterations.class) final int maxIters,
      @Parameter(EnableRampup.class) final boolean rampup,
      final StepSizes ts) {

    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ts = ts;
    this.ignoreAndContinue = rampup;
    this.model = new DenseVector(dimensions);
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
    this.modelAndDescentDirectionBroadcaster =
        communicationGroupClient.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
    this.descentDriectionBroadcaster = communicationGroupClient.getBroadcastSender(DescentDirectionBroadcaster.class);
    this.lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroupClient.getBroadcastSender(MinEtaBroadcaster.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    double gradientNorm = Double.MAX_VALUE;
    for (int iteration = 1; !converged(iteration, gradientNorm); ++iteration) {
      try (Timer t = new Timer("Current Iteration(" + iteration + ")")) {
        final Pair<Double, Vector> lossAndGradient = computeLossAndGradient();
        losses.add(lossAndGradient.getFirst());
        final Vector descentDirection = getDescentDirection(lossAndGradient.getSecond());

        updateModel(descentDirection);

        gradientNorm = descentDirection.norm2();
      }
    }
    LOG.log(Level.INFO, "OUT: Stop");
    controlMessageBroadcaster.send(ControlMessages.Stop);

    for (final Double loss : losses) {
      LOG.log(Level.INFO, "OUT: LOSS = {0}", loss);
    }
    return lossCodec.encode(losses);
  }

  private void updateModel(final Vector descentDirection) throws NetworkException, InterruptedException {
    try (Timer t = new Timer("GetDescentDirection + FindMinEta + UpdateModel")) {
      final Vector lineSearchEvals = lineSearch(descentDirection);
      minEta = findMinEta(model, descentDirection, lineSearchEvals);
      model.multAdd(minEta, descentDirection);
    }

    LOG.log(Level.INFO, "OUT: New Model = {0}", model);
  }

  private Vector lineSearch(final Vector descentDirection) throws NetworkException, InterruptedException {
    Vector lineSearchResults = null;
    boolean allDead = false;
    do {
      try (Timer t = new Timer("LineSearch - Broadcast("
          + (sendModel ? "ModelAndDescentDirection" : "DescentDirection") + ") + Reduce(LossEvalsInLineSearch)")) {
        if (sendModel) {
          LOG.log(Level.INFO, "OUT: DoLineSearchWithModel");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearchWithModel);
          modelAndDescentDirectionBroadcaster.send(new Pair<>(model, descentDirection));
        } else {
          LOG.log(Level.INFO, "OUT: DoLineSearch");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
          descentDriectionBroadcaster.send(descentDirection);
        }
        final Pair<Vector, Integer> lineSearchEvals = lineSearchEvaluationsReducer.reduce();
        if (lineSearchEvals != null) {
          final int numExamples = lineSearchEvals.getSecond();
          lineSearchResults = lineSearchEvals.getFirst();
          lineSearchResults.scale(1.0 / numExamples);
          LOG.log(Level.INFO, "OUT: #Examples: {0}", numExamples);
          LOG.log(Level.INFO, "OUT: LineSearchEvals: {0}", lineSearchResults);
          allDead = false;
        } else {
          allDead = true;
        }
      }

      sendModel = chkAndUpdate();
    } while (allDead || !ignoreAndContinue && sendModel);
    return lineSearchResults;
  }

  private Pair<Double, Vector> computeLossAndGradient() throws NetworkException, InterruptedException {
    Pair<Double, Vector> returnValue = null;
    boolean allDead = false;
    do {
      try (Timer t = new Timer("Broadcast(" + (sendModel ? "Model" : "MinEta") + ") + Reduce(LossAndGradient)")) {
        if (sendModel) {
          LOG.log(Level.INFO, "OUT: ComputeGradientWithModel");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithModel);
          modelBroadcaster.send(model);
        } else {
          LOG.log(Level.INFO, "OUT: ComputeGradientWithMinEta");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithMinEta);
          minEtaBroadcaster.send(minEta);
        }
        final Pair<Pair<Double, Integer>, Vector> lossAndGradient = lossAndGradientReducer.reduce();

        if (lossAndGradient != null) {
          final int numExamples = lossAndGradient.getFirst().getSecond();
          LOG.log(Level.INFO, "OUT: #Examples: {0}", numExamples);
          final double lossPerExample = lossAndGradient.getFirst().getFirst() / numExamples;
          LOG.log(Level.INFO, "OUT: Loss: {0}", lossPerExample);
          final double objFunc = (lambda / 2) * model.norm2Sqr() + lossPerExample;
          LOG.log(Level.INFO, "OUT: Objective Func Value: {0}", objFunc);
          final Vector gradient = lossAndGradient.getSecond();
          gradient.scale(1.0 / numExamples);
          LOG.log(Level.INFO, "OUT: Gradient: {0}", gradient);
          returnValue = new Pair<>(objFunc, gradient);
          allDead = false;
        } else {
          allDead = true;
        }
      }
      sendModel = chkAndUpdate();
    } while (allDead || !ignoreAndContinue && sendModel);
    return returnValue;
  }

  private boolean chkAndUpdate() {
    long t1 = System.currentTimeMillis();
    final GroupChanges changes = communicationGroupClient.getTopologyChanges();
    long t2 = System.currentTimeMillis();
    LOG.log(Level.INFO, "OUT: Time to get TopologyChanges = " + (t2 - t1) / 1000.0 + " sec");
    if (changes.exist()) {
      LOG.log(Level.INFO, "OUT: There exist topology changes. Asking to update Topology");
      t1 = System.currentTimeMillis();
      communicationGroupClient.updateTopology();
      t2 = System.currentTimeMillis();
      LOG.log(Level.INFO, "OUT: Time to get TopologyChanges = " + (t2 - t1) / 1000.0 + " sec");
      return true;
    } else {
      LOG.log(Level.INFO, "OUT: No changes in topology exist. So not updating topology");
      return false;
    }
  }

  private boolean converged(final int iters, final double gradNorm) {
    return iters >= maxIters || Math.abs(gradNorm) <= 1e-3;
  }

  private double findMinEta(final Vector theModel, final Vector descentDir, final Vector lineSearchEvals) {
    final double wNormSqr = theModel.norm2Sqr();
    final double dNormSqr = descentDir.norm2Sqr();
    final double wDotd = theModel.dot(descentDir);
    final double[] t = ts.getT();
    int i = 0;
    for (final double eta : t) {
      final double modelNormSqr = wNormSqr + (eta * eta) * dNormSqr + 2 * eta * wDotd;
      final double loss = lineSearchEvals.get(i) + ((lambda / 2) * modelNormSqr);
      lineSearchEvals.set(i, loss);
      ++i;
    }
    LOG.log(Level.INFO, "OUT: Regularized LineSearchEvals: {0}", lineSearchEvals);
    final Tuple<Integer, Double> minTup = lineSearchEvals.min();
    LOG.log(Level.INFO, "OUT: MinTup: {0}", minTup);
    final double minT = t[minTup.getKey()];
    LOG.log(Level.INFO, "OUT: MinT: {0}", minT);
    return minT;
  }

  private Vector getDescentDirection(final Vector gradient) {
    gradient.multAdd(lambda, model);
    gradient.scale(-1);
    LOG.log(Level.INFO, "OUT: DescentDirection: {0}", gradient);
    return gradient;
  }
}
