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
import org.apache.reef.driver.parameters.DriverRestartEvaluatorRecoverySeconds;
import org.apache.reef.driver.parameters.ServiceDriverRestartCompletedHandlers;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.runtime.common.driver.idle.DriverIdlenessSource;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceRecoverEvent;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

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
  private final Set<EventHandler<DriverRestartCompleted>> driverRestartCompletedHandlers;
  private final Set<EventHandler<DriverRestartCompleted>> serviceDriverRestartCompletedHandlers;
  private final int driverRestartEvaluatorRecoverySeconds;
  private final Timer restartCompletedTimer = new Timer();

  private RestartEvaluators restartEvaluators;
  private DriverRestartState state = DriverRestartState.NOT_RESTARTED;
  private int resubmissionAttempts = 0;

  @Inject
  private DriverRestartManager(final DriverRuntimeRestartManager driverRuntimeRestartManager,
                               @Parameter(DriverRestartEvaluatorRecoverySeconds.class)
                               final int driverRestartEvaluatorRecoverySeconds,
                               @Parameter(DriverRestartCompletedHandlers.class)
                               final Set<EventHandler<DriverRestartCompleted>> driverRestartCompletedHandlers,
                               @Parameter(ServiceDriverRestartCompletedHandlers.class)
                               final Set<EventHandler<DriverRestartCompleted>> serviceDriverRestartCompletedHandlers) {
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
    this.driverRestartCompletedHandlers = driverRestartCompletedHandlers;
    this.serviceDriverRestartCompletedHandlers = serviceDriverRestartCompletedHandlers;
    if (driverRestartEvaluatorRecoverySeconds < 0) {
      throw new IllegalArgumentException("driverRestartEvaluatorRecoverySeconds must be greater than 0.");
    }

    this.driverRestartEvaluatorRecoverySeconds = driverRestartEvaluatorRecoverySeconds;
  }

  /**
   * Triggers the state machine if the application is a restart instance. Returns true
   * @return true if the application is a restart instance.
   * Can be already done with restart or in the process of restart.
   */
  public synchronized boolean detectRestart() {
    if (this.state.hasNotRestarted()) {
      resubmissionAttempts = driverRuntimeRestartManager.getResubmissionAttempts();

      if (resubmissionAttempts > 0) {
        // set the state machine in motion.
        this.state = DriverRestartState.BEGAN;
      }
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
   * Recovers the list of alive and failed evaluators and inform the driver restart handlers and inform the
   * evaluator failure handlers based on the specific runtime. Also sets the expected amount of evaluators to report
   * back as alive to the job driver.
   */
  public synchronized void onRestart(final StartTime startTime,
                                     final List<EventHandler<DriverRestarted>> orderedHandlers) {
    if (this.state == DriverRestartState.BEGAN) {
      restartEvaluators = driverRuntimeRestartManager.getPreviousEvaluators();
      final DriverRestarted restartedInfo = new DriverRestartedImpl(resubmissionAttempts, startTime, restartEvaluators);

      for (final EventHandler<DriverRestarted> handler : orderedHandlers) {
        handler.onNext(restartedInfo);
      }

      this.state = DriverRestartState.IN_PROGRESS;
    } else {
      final String errMsg = "Should not be setting the set of expected alive evaluators more than once.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    driverRuntimeRestartManager.informAboutEvaluatorFailures(getFailedEvaluators());

    if (driverRestartEvaluatorRecoverySeconds != Integer.MAX_VALUE) {
      // Don't use Clock here because if there is an event scheduled, the driver will not be idle, even if
      // driver restart has already completed, and we cannot cancel the event.
      restartCompletedTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          onDriverRestartCompleted(true);
        }
      }, driverRestartEvaluatorRecoverySeconds * 1000L);
    }
  }

  /**
   * @return The restart state of the specified evaluator. Returns {@link EvaluatorRestartState#NOT_EXPECTED}
   * if the {@link DriverRestartManager} does not believe that it's an evaluator to be recovered.
   */
  public synchronized EvaluatorRestartState getEvaluatorRestartState(final String evaluatorId) {
    if (this.state.hasNotRestarted()) {
      return EvaluatorRestartState.NOT_EXPECTED;
    }

    return getStateOfPreviousEvaluator(evaluatorId);
  }

  /**
   * @return The ResourceRecoverEvent of the specified evaluator. Throws a {@link DriverFatalRuntimeException} if
   * the evaluator does not exist in the set of known evaluators.
   */
  public synchronized ResourceRecoverEvent getResourceRecoverEvent(final String evaluatorId) {
    if (!this.restartEvaluators.contains(evaluatorId)) {
      throw new DriverFatalRuntimeException("Unexpected evaluator [" + evaluatorId + "], should " +
          "not have been recorded.");
    }

    return this.restartEvaluators.get(evaluatorId).getResourceRecoverEvent();
  }

  /**
   * Indicate that this Driver has re-established the connection with one more Evaluator of a previous run.
   * @return true if the evaluator has been newly recovered.
   */
  public synchronized boolean onRecoverEvaluator(final String evaluatorId) {
    if (getStateOfPreviousEvaluator(evaluatorId).isFailedOrNotExpected()) {
      final String errMsg = "Evaluator with evaluator ID " + evaluatorId + " not expected to be alive.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    if (getStateOfPreviousEvaluator(evaluatorId) != EvaluatorRestartState.EXPECTED) {
      LOG.log(Level.WARNING, "Evaluator with evaluator ID " + evaluatorId + " added to the set" +
          " of recovered evaluators more than once. Ignoring second add...");
      return false;
    }

    // set the status for this evaluator ID to be reported.
    setEvaluatorReported(evaluatorId);

    if (haveAllExpectedEvaluatorsReported()) {
      onDriverRestartCompleted(false);
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
   * Signals to the {@link DriverRestartManager} that an evaluator has reported back after restart.
   */
  public synchronized void setEvaluatorReported(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.REPORTED);
  }

  /**
   * Signals to the {@link DriverRestartManager} that an evaluator has had its recovery heartbeat processed.
   */
  public synchronized void setEvaluatorReregistered(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.REREGISTERED);
  }

  /**
   * Signals to the {@link DriverRestartManager} that an evaluator has had its running task or active context processed.
   */
  public synchronized void setEvaluatorProcessed(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.PROCESSED);
  }

  /**
   * Signals to the {@link DriverRestartManager} that an expected evaluator has been expired.
   */
  public synchronized void setEvaluatorExpired(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.EXPIRED);
  }

  private synchronized EvaluatorRestartState getStateOfPreviousEvaluator(final String evaluatorId) {
    if (!this.restartEvaluators.contains(evaluatorId)) {
      return EvaluatorRestartState.NOT_EXPECTED;
    }

    return this.restartEvaluators.get(evaluatorId).getEvaluatorRestartState();
  }

  private synchronized void setStateOfPreviousEvaluator(final String evaluatorId,
                                                        final EvaluatorRestartState to) {
    if (!restartEvaluators.contains(evaluatorId) ||
        !restartEvaluators.get(evaluatorId).setEvaluatorRestartState(to)) {
      throw evaluatorTransitionFailed(evaluatorId, to);
    }
  }

  private synchronized DriverFatalRuntimeException evaluatorTransitionFailed(final String evaluatorId,
                                                                             final EvaluatorRestartState to) {
    if (!restartEvaluators.contains(evaluatorId)) {
      return new DriverFatalRuntimeException("Evaluator " + evaluatorId + " is not expected.");
    }

    return new DriverFatalRuntimeException("Evaluator " + evaluatorId + " wants to transition to state " +
        "[" + to + "], but is in the illegal state [" +
        restartEvaluators.get(evaluatorId).getEvaluatorRestartState() + "].");
  }

  private synchronized boolean haveAllExpectedEvaluatorsReported() {
    for (final String previousEvaluatorId : this.restartEvaluators.getEvaluatorIds()) {
      final EvaluatorRestartState restartState = getStateOfPreviousEvaluator(previousEvaluatorId);
      if (restartState == EvaluatorRestartState.EXPECTED) {
        return false;
      }
    }

    return true;
  }

  /**
   * Sets the driver restart status to be completed if not yet set and notifies the restart completed event handlers.
   */
  private synchronized void onDriverRestartCompleted(final boolean isTimedOut) {
    if (this.state != DriverRestartState.COMPLETED) {
      final Set<String> outstandingEvaluatorIds = getOutstandingEvaluatorsAndMarkExpired();
      driverRuntimeRestartManager.informAboutEvaluatorFailures(outstandingEvaluatorIds);

      this.state = DriverRestartState.COMPLETED;
      final DriverRestartCompleted driverRestartCompleted = new DriverRestartCompletedImpl(
          System.currentTimeMillis(), isTimedOut);

      for (final EventHandler<DriverRestartCompleted> serviceRestartCompletedHandler
          : this.serviceDriverRestartCompletedHandlers) {
        serviceRestartCompletedHandler.onNext(driverRestartCompleted);
      }

      for (final EventHandler<DriverRestartCompleted> restartCompletedHandler : this.driverRestartCompletedHandlers) {
        restartCompletedHandler.onNext(driverRestartCompleted);
      }

      LOG.log(Level.FINE, "Restart completed. Evaluators that have not reported back are: " + outstandingEvaluatorIds);
    }

    restartCompletedTimer.cancel();
  }

  /**
   * Gets the outstanding evaluators that have not yet reported back and mark them as expired.
   */
  private Set<String> getOutstandingEvaluatorsAndMarkExpired() {
    final Set<String> outstanding = new HashSet<>();
    for (final String previousEvaluatorId : restartEvaluators.getEvaluatorIds()) {
      if (getStateOfPreviousEvaluator(previousEvaluatorId) == EvaluatorRestartState.EXPECTED) {
        outstanding.add(previousEvaluatorId);
        setEvaluatorExpired(previousEvaluatorId);
      }
    }

    return outstanding;
  }

  private Set<String> getFailedEvaluators() {
    final Set<String> failed = new HashSet<>();
    for (final String previousEvaluatorId : this.restartEvaluators.getEvaluatorIds()) {
      if (getStateOfPreviousEvaluator(previousEvaluatorId) == EvaluatorRestartState.FAILED) {
        failed.add(previousEvaluatorId);
      }
    }

    return failed;
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
}
