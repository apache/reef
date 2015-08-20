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
package org.apache.reef.driver.restart;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverRestartCompletedHandlers;
import org.apache.reef.driver.parameters.DriverRestartEvaluatorRecoveryMs;
import org.apache.reef.driver.parameters.ServiceDriverRestartCompletedHandlers;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.DriverRestartCompleted;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManagerFactory;
import org.apache.reef.runtime.common.driver.idle.DriverIdlenessSource;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The manager that handles aspects of driver restart such as determining whether the driver is in
 * restart mode, what to do on restart, whether restart is completed, and others.
 */
@DriverSide
@Private
@Unstable
public final class DriverRestartManager implements DriverIdlenessSource {
  private static final String CLASS_NAME = DriverRestartManager.class.getName();
  private static final Logger LOG = Logger.getLogger(CLASS_NAME);

  private final DriverRuntimeRestartManager driverRuntimeRestartManager;
  private final Map<String, EvaluatorRestartInfo> previousEvaluators;
  private final int driverRestartEvaluatorRecoveryMs;
  private final EvaluatorManagerFactory evaluatorManagerFactory;
  private final Set<EventHandler<DriverRestartCompleted>> driverRestartCompletedHandlers;
  private final Set<EventHandler<DriverRestartCompleted>> serviceDriverRestartCompletedHandlers;
  private final Clock clock;

  private DriverRestartState state;

  @Inject
  private DriverRestartManager(@Parameter(DriverRestartEvaluatorRecoveryMs.class)
                               final int driverRestartEvaluatorRecoveryMs,
                               final Clock clock,
                               final DriverRuntimeRestartManager driverRuntimeRestartManager,
                               final EvaluatorManagerFactory evaluatorManagerFactory,
                               @Parameter(DriverRestartCompletedHandlers.class)
                               final Set<EventHandler<DriverRestartCompleted>> driverRestartCompletedHandlers,
                               @Parameter(ServiceDriverRestartCompletedHandlers.class)
                               final Set<EventHandler<DriverRestartCompleted>> serviceDriverRestartCompletedHandlers) {
    this.clock = clock;
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
    this.state = DriverRestartState.NotRestarted;
    this.driverRestartEvaluatorRecoveryMs = driverRestartEvaluatorRecoveryMs;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
    this.driverRestartCompletedHandlers = driverRestartCompletedHandlers;
    this.serviceDriverRestartCompletedHandlers = serviceDriverRestartCompletedHandlers;
    this.previousEvaluators = new HashMap<>();
    if (driverRestartEvaluatorRecoveryMs <= 0) {
      throw new DriverFatalRuntimeException("The restart wait time for the driver must be greater than 0.");
    }
  }

  /**
   * Triggers the state machine if the application is a restart instance. Returns true
   * @return true if the application is a restart instance.
   */
  public synchronized boolean detectRestart() {
    if (!this.state.hasRestarted() && driverRuntimeRestartManager.hasRestarted()) {
      this.state = DriverRestartState.RestartBegan;
    }

    return this.state.hasRestarted();
  }

  /**
   * @return true if the driver is undergoing the process of restart.
   */
  public synchronized boolean isRestarting() {
    return this.state.isRestarting();
  }

  /**
   * Recovers the list of alive and failed evaluators and inform about evaluator failures
   * based on the specific runtime. Also sets the expected amount of evaluators to report back
   * as alive to the job driver and schedules the restart clock.
   */
  public synchronized void onRestart() {
    final EvaluatorRestartCollection evaluatorRestartInfo = driverRuntimeRestartManager.getAliveAndFailedEvaluators();
    setPreviousEvaluators(evaluatorRestartInfo.getAliveEvaluators());
    driverRuntimeRestartManager.informAboutEvaluatorFailures(evaluatorRestartInfo.getFailedEvaluators());
    if (driverRestartEvaluatorRecoveryMs != Integer.MAX_VALUE) {
      // Don't use Clock here because if there is an event scheduled, the driver will not be idle, even if
      // driver restart has already completed, and we cannot cancel the event.
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          onDriverRestartCompleted();
        }
      }, driverRestartEvaluatorRecoveryMs);
    }
  }

  /**
   * @return whether restart is completed.
   */
  public synchronized boolean isRestartCompleted() {
    return this.state == DriverRestartState.RestartCompleted;
  }

  /**
   * @return the restart state of the evaluator.
   */
  public synchronized EvaluatorRestartState getEvaluatorRestartState(final String evaluatorId) {
    if (!this.state.hasRestarted() ||
        !this.previousEvaluators.containsKey(evaluatorId)) {
      return EvaluatorRestartState.NOT_RESTARTED_EVALUATOR;
    }

    return this.previousEvaluators.get(evaluatorId).getState();
  }

  /**
   * @return the Evaluators expected to check in from a previous run.
   */
  public synchronized Map<String, EvaluatorRestartInfo> getPreviousEvaluators() {
    return Collections.unmodifiableMap(this.previousEvaluators);
  }

  /**
   * Set the Evaluators to expect still active from a previous execution of the Driver in a restart situation.
   * To be called exactly once during a driver restart.
   * @param evaluatorMap the evaluator IDs of the evaluators that are expected to have survived driver restart.
   */
  public synchronized void setPreviousEvaluators(final Map<String, EvaluatorRestartInfo> evaluatorMap) {
    if (this.state == DriverRestartState.RestartBegan) {
      for (final Map.Entry<String, EvaluatorRestartInfo> entry : evaluatorMap.entrySet()) {
        previousEvaluators.put(entry.getKey(), entry.getValue());
      }

      this.state = DriverRestartState.RestartInProgress;
    } else {
      final String errMsg = "Should not be setting the set of expected alive evaluators more than once.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }
  }

  /**
   * Indicate that this Driver has re-established the connection with one more Evaluator of a previous run.
   * @return true if the evaluator has been newly recovered.
   */
  public synchronized boolean onRecoverEvaluator(final String evaluatorId) {
    if (!this.previousEvaluators.containsKey(evaluatorId)) {
      final String errMsg = "Evaluator with evaluator ID " + evaluatorId + " not expected to be alive.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    if (this.previousEvaluators.get(evaluatorId).getState() != EvaluatorRestartState.EXPECTED) {
      LOG.log(Level.WARNING, "Evaluator with evaluator ID " + evaluatorId + " added to the set" +
          " of recovered evaluators more than once. Ignoring second add...");
      return false;
    }

    // set the status for this evaluator ID to be reported.
    this.previousEvaluators.get(evaluatorId).setState(EvaluatorRestartState.REPORTED);

    boolean restartCompleted = true;

    for (final EvaluatorRestartInfo restartInfo : this.previousEvaluators.values()) {
      if (!restartInfo.getState().hasReported()) {
        restartCompleted = false;
      }
    }

    if (restartCompleted) {
      onDriverRestartCompleted();
    }

    return true;
  }

  /**
   * Records the evaluators when it is allocated. The implementation depends on the runtime.
   * @param id The evaluator ID of the allocated evaluator.
   */
  public synchronized void recordAllocatedEvaluator(final String id) {
    driverRuntimeRestartManager.recordAllocatedEvaluator(id);
  }

  /**
   * Records a removed evaluator into the evaluator log. The implementation depends on the runtime.
   * @param id The evaluator ID of the removed evaluator.
   */
  public synchronized void recordRemovedEvaluator(final String id) {
    driverRuntimeRestartManager.recordRemovedEvaluator(id);
  }

  /**
   * {@inheritDoc}
   * @return True if not in process of restart. False otherwise.
   */
  @Override
  public IdleMessage getIdleStatus() {
    boolean idleState = !this.state.isRestarting();
    final String idleMessage = idleState ? CLASS_NAME + " currently not in the process of restart." :
        CLASS_NAME + " currently in the process of restart.";
    return new IdleMessage(CLASS_NAME, idleMessage, idleState);
  }

  /**
   * Sets the driver restart status to be completed if not yet set and notifies the restart completed event handlers.
   */
  private synchronized void onDriverRestartCompleted() {
    if (this.state != DriverRestartState.RestartCompleted) {
      final Set<String> outstandingEvaluatorIds = getOutstandingEvaluatorsAndMarkExpired();
      driverRuntimeRestartManager.informAboutEvaluatorFailures(outstandingEvaluatorIds);
      closeUnrecoverableEvaluators(outstandingEvaluatorIds);

      this.state = DriverRestartState.RestartCompleted;
      final DriverRestartCompleted driverRestartCompleted = new DriverRestartCompleted(System.currentTimeMillis());

      for (final EventHandler<DriverRestartCompleted> serviceRestartCompletedHandler
          : this.serviceDriverRestartCompletedHandlers) {
        serviceRestartCompletedHandler.onNext(driverRestartCompleted);
      }

      for (final EventHandler<DriverRestartCompleted> restartCompletedHandler : this.driverRestartCompletedHandlers) {
        restartCompletedHandler.onNext(driverRestartCompleted);
      }

      LOG.log(Level.FINE, "Restart completed. Evaluators that have not reported back are: " + outstandingEvaluatorIds);
    }
  }

  /**
   * Gets the outstanding evaluators that have not yet reported back and mark them as expired.
   */
  private Set<String> getOutstandingEvaluatorsAndMarkExpired() {
    final Set<String> outstanding = new HashSet<>();
    for (final Map.Entry<String, EvaluatorRestartInfo> entry : previousEvaluators.entrySet()) {
      if (!entry.getValue().getState().hasReported()) {
        outstanding.add(entry.getKey());
        entry.getValue().setState(EvaluatorRestartState.EXPIRED);
      }
    }

    return outstanding;
  }

  /**
   * Closes the evaluator properly for unrecoverable evaluators.
   */
  private void closeUnrecoverableEvaluators(final Set<String> evaluatorIds) {
    for (final String evaluatorId : evaluatorIds) {
      final ResourceStatusEvent recoveredResourceStatusEvent =
          ResourceStatusEventImpl.newBuilder().setIdentifier(evaluatorId).setIsFromPreviousDriver(true)
              .setState(ReefServiceProtos.State.FAILED).build();

      evaluatorManagerFactory.getNewEvaluatorManagerForRecoveredEvaluatorDuringDriverRestart(
          this.previousEvaluators.get(evaluatorId).getEvaluatorInfo(), recoveredResourceStatusEvent).close();
    }
  }
}
