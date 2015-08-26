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
import org.apache.reef.exception.DriverFatalRuntimeException;

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
public final class DriverRestartManager {
  private static final Logger LOG = Logger.getLogger(DriverRestartManager.class.getName());
  private final DriverRuntimeRestartManager driverRuntimeRestartManager;

  private RestartEvaluators restartEvaluators;
  private DriverRestartState state = DriverRestartState.NOT_RESTARTED;

  @Inject
  private DriverRestartManager(final DriverRuntimeRestartManager driverRuntimeRestartManager) {
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
  }

  /**
   * Triggers the state machine if the application is a restart instance. Returns true
   * @return true if the application is a restart instance.
   * Can be already done with restart or in the process of restart.
   */
  public synchronized boolean detectRestart() {
    if (this.state.hasNotRestarted() && driverRuntimeRestartManager.hasRestarted()) {
      // set the state machine in motion.
      this.state = DriverRestartState.BEGAN;
    }

    return this.state.hasRestarted();
  }

  /**
   * @return true if the application is a restart instance.
   * Can be already done with restart or in the process of restart.
   */
  public synchronized boolean hasRestarted() {
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
   * as alive to the job driver.
   */
  public synchronized void onRestart() {
    if (this.state == DriverRestartState.BEGAN) {
      restartEvaluators = driverRuntimeRestartManager.getPreviousEvaluators();
      this.state = DriverRestartState.IN_PROGRESS;
    } else {
      final String errMsg = "Should not be setting the set of expected alive evaluators more than once.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    driverRuntimeRestartManager.informAboutEvaluatorFailures(getFailedEvaluators());

    // TODO[REEF-560]: Call onDriverRestartCompleted() (to do in REEF-617) on a Timer.
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
   * Indicate that this Driver has re-established the connection with one more Evaluator of a previous run.
   * Calls the restart complete action if the latest evaluator is the last evaluator to recover.
   * @return true if the driver restart is completed.
   */
  public synchronized boolean onRecoverEvaluatorIsRestartComplete(final String evaluatorId) {
    if (getStateOfPreviousEvaluator(evaluatorId) == EvaluatorRestartState.NOT_EXPECTED) {
      final String errMsg = "Evaluator with evaluator ID " + evaluatorId + " not expected to be alive.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    if (getStateOfPreviousEvaluator(evaluatorId) != EvaluatorRestartState.EXPECTED) {
      LOG.log(Level.WARNING, "Evaluator with evaluator ID " + evaluatorId + " added to the set" +
          " of recovered evaluators more than once. Ignoring second add...");
    } else {
      setEvaluatorReported(evaluatorId);
    }

    return haveAllExpectedEvaluatorsReported();
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
   * Signals to the {@link DriverRestartManager} that an evaluator has had its running task processed.
   */
  public synchronized void setEvaluatorRunningTask(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.PROCESSED);
  }

  /**
   * Signals to the {@link DriverRestartManager} that an expected evaluator has been expired.
   */
  public synchronized void setEvaluatorExpired(final String evaluatorId) {
    setStateOfPreviousEvaluator(evaluatorId, EvaluatorRestartState.EXPIRED);
  }

  private synchronized EvaluatorRestartState getStateOfPreviousEvaluator(final String evaluatorId) {
    if (this.restartEvaluators.contains(evaluatorId)) {
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

  private Set<String> getFailedEvaluators() {
    final Set<String> failed = new HashSet<>();
    for (final String previousEvaluatorId : this.restartEvaluators.getEvaluatorIds()) {
      if (getStateOfPreviousEvaluator(previousEvaluatorId) == EvaluatorRestartState.FAILED) {
        failed.add(previousEvaluatorId);
      }
    }

    return failed;
  }
}
