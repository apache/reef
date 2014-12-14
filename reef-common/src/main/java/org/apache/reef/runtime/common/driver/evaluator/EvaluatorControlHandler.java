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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles the sending of Evaluator control messages to the Evaluator.
 */
@DriverSide
@Private
public final class EvaluatorControlHandler {

  private static Logger LOG = Logger.getLogger(EvaluatorControlHandler.class.getName());
  private final EvaluatorStatusManager stateManager;
  private final RemoteManager remoteManager;
  private final String evaluatorId;
  private Optional<EventHandler<EvaluatorRuntimeProtocol.EvaluatorControlProto>> wrapped = Optional.empty();

  /**
   * @param stateManager  used to check whether the Evaluator is running before sending a message.
   * @param remoteManager used to establish the communications link as soon as the remote ID has been set.
   */
  @Inject
  EvaluatorControlHandler(final EvaluatorStatusManager stateManager,
                          final RemoteManager remoteManager,
                          final @Parameter(EvaluatorManager.EvaluatorIdentifier.class) String evaluatorId) {
    this.stateManager = stateManager;
    this.remoteManager = remoteManager;
    this.evaluatorId = evaluatorId;
    LOG.log(Level.FINE, "Instantiated 'EvaluatorControlHandler'");
  }

  /**
   * Send the evaluatorControlProto to the Evaluator.
   *
   * @param evaluatorControlProto
   * @throws java.lang.IllegalStateException if the remote ID hasn't been set via setRemoteID() prior to this call
   * @throws java.lang.IllegalStateException if the Evaluator isn't running.
   */
  public synchronized void send(final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto) {
    if (!this.wrapped.isPresent()) {
      throw new IllegalStateException("Trying to send an EvaluatorControlProto before the Evaluator ID is set.");
    }
    if (!this.stateManager.isRunning()) {
      final String msg = new StringBuilder()
          .append("Trying to send an EvaluatorControlProto to Evaluator [")
          .append(this.evaluatorId)
          .append("] that is in state [")
          .append(this.stateManager.toString())
          .append("], not [RUNNING]. The control message was: ")
          .append(evaluatorControlProto.toString())
          .toString();
      throw new IllegalStateException(msg);
    }
    this.wrapped.get().onNext(evaluatorControlProto);
  }

  /**
   * Set the remote ID used to communicate with this Evaluator.
   *
   * @param evaluatorRID
   * @throws java.lang.IllegalStateException if the remote ID has been set before.
   */
  synchronized void setRemoteID(final String evaluatorRID) {
    if (this.wrapped.isPresent()) {
      throw new IllegalStateException("Trying to reset the evaluator ID. This isn't supported.");
    } else {
      LOG.log(Level.FINE, "Registering remoteId [{0}] for Evaluator [{1}]", new Object[]{evaluatorRID, evaluatorId});
      this.wrapped = Optional.of(remoteManager.getHandler(evaluatorRID, EvaluatorRuntimeProtocol.EvaluatorControlProto.class));
    }
  }

}
