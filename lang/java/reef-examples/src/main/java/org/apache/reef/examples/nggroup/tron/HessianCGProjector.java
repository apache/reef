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
package org.apache.reef.examples.nggroup.tron;

import org.apache.reef.examples.nggroup.tron.operations.CG.CGDirectionProjector;
import org.apache.reef.examples.nggroup.tron.operatornames.ConjugateDirectionBroadcaster;
import org.apache.reef.examples.nggroup.tron.operatornames.LossSecondDerivativeCompletionReducer;
import org.apache.reef.examples.nggroup.tron.operatornames.ProjectedDirectionReducer;
import org.apache.reef.examples.nggroup.utils.math.Vector;
import org.apache.reef.examples.nggroup.utils.timer.Timer;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.operators.Broadcast;
import org.apache.reef.io.network.group.operators.Broadcast.Sender;
import org.apache.reef.io.network.group.operators.Reduce;
import org.apache.reef.io.network.group.operators.Reduce.Receiver;
import org.apache.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.util.Pair;

import java.util.logging.Level;
import java.util.logging.Logger;

public class HessianCGProjector implements CGDirectionProjector {

  private static final Logger LOG = Logger.getLogger(HessianCGProjector.class.getName());

  private final Sender<Vector> conjugateDirectionBroadcaster;
  private final Receiver<Pair<Integer, Vector>> projectedDirectionReducer;
  private final Sender<ControlMessages> controlMessageBroadcaster;
  private final Reduce.Receiver<Boolean> lossSecDerCompletionReducer;
  private final Vector model;
  private final Sender<Vector> modelBroadcaster;
  private final double lambda;
  private boolean sendModel = false;
  private final CommunicationGroupClient communicationGroupClient;
  private final boolean ignoreAndContinue;

  public HessianCGProjector(final Vector model, final double lambda, final boolean ignoreAndContinue,
                            final CommunicationGroupClient communicationGroupClient,
                            final Broadcast.Sender<ControlMessages> controlMessageBroadcaster,
                            final Broadcast.Sender<Vector> modelBroadcaster) {
    this.model = model;
    this.lambda = lambda;
    this.ignoreAndContinue = ignoreAndContinue;
    this.communicationGroupClient = communicationGroupClient;
    this.controlMessageBroadcaster = controlMessageBroadcaster;
    this.modelBroadcaster = modelBroadcaster;
    this.conjugateDirectionBroadcaster = communicationGroupClient.getBroadcastSender(ConjugateDirectionBroadcaster.class);
    this.projectedDirectionReducer = communicationGroupClient.getReduceReceiver(ProjectedDirectionReducer.class);
    this.lossSecDerCompletionReducer = communicationGroupClient.getReduceReceiver(LossSecondDerivativeCompletionReducer.class);
  }

  @Override
  public void project(final Vector CGDirection, final Vector projectedCGDirection) throws NetworkException, InterruptedException {
    boolean allDead = false;
    do {
      try (final Timer t = new Timer(sendModel ? "Broadcast(Model), " : "" + "Broadcast(CGDirection) + Reduce(LossAndGradient)")) {
        if (sendModel) {
          LOG.log(Level.INFO, "OUT: ComputeLossSecondDerivativeWithModel");
          controlMessageBroadcaster.send(ControlMessages.ComputeLossSecondDerivativeWithModel);
          modelBroadcaster.send(model);
          lossSecDerCompletionReducer.reduce();
        }
        LOG.log(Level.INFO, "OUT: ComputeProjectionDirection");
        controlMessageBroadcaster.send(ControlMessages.ComputeProjectionDirection);
        conjugateDirectionBroadcaster.send(CGDirection);

        final Pair<Integer, Vector> projectedDirection = projectedDirectionReducer.reduce();
        if (projectedDirection != null) {
          final int numExamples = projectedDirection.first;
          LOG.log(Level.INFO, "OUT: #Examples: {0}", numExamples);
          projectedCGDirection.scale(0);
          projectedCGDirection.add(projectedDirection.second);
          projectedCGDirection.scale(1.0 / numExamples);
          projectedCGDirection.multAdd(lambda, CGDirection);
          allDead = false;
        } else {
          allDead = true;
        }
        sendModel = SynchronizationUtils.chkAndUpdate(this.communicationGroupClient);
      }
    } while (allDead || (!ignoreAndContinue && sendModel));
  }
}
