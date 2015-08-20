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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
  private final Set<String> previousEvaluators;
  private final Set<String> recoveredEvaluators;
  private DriverRestartState state;

  @Inject
  private DriverRestartManager(final DriverRuntimeRestartManager driverRuntimeRestartManager) {
    this.driverRuntimeRestartManager = driverRuntimeRestartManager;
    this.state = DriverRestartState.NotRestarted;
    this.previousEvaluators = new HashSet<>();
    this.recoveredEvaluators = new HashSet<>();
  }

  /**
   * Triggers the state machine if the application is a restart instance. Returns true
   * @return true if the application is a restart instance.
   * Can be already done with restart or in the process of restart.
   */
  public synchronized boolean detectRestart() {
    if (!this.state.hasRestarted() && driverRuntimeRestartManager.hasRestarted()) {
      this.state = DriverRestartState.RestartBegan;
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
    final EvaluatorRestartInfo evaluatorRestartInfo = driverRuntimeRestartManager.getAliveAndFailedEvaluators();
    setPreviousEvaluatorIds(evaluatorRestartInfo.getAliveEvaluators());
    driverRuntimeRestartManager.informAboutEvaluatorFailures(evaluatorRestartInfo.getFailedEvaluators());
  }

  /**
   * @return whether restart is completed.
   */
  public synchronized boolean isRestartCompleted() {
    return this.state == DriverRestartState.RestartCompleted;
  }

  /**
   * @return the Evaluators expected to check in from a previous run.
   */
  public synchronized Set<String> getPreviousEvaluatorIds() {
    return Collections.unmodifiableSet(this.previousEvaluators);
  }

  /**
   * Set the Evaluators to expect still active from a previous execution of the Driver in a restart situation.
   * To be called exactly once during a driver restart.
   *
   * @param ids the evaluator IDs of the evaluators that are expected to have survived driver restart.
   */
  public synchronized void setPreviousEvaluatorIds(final Set<String> ids) {
    if (this.state != DriverRestartState.RestartInProgress) {
      previousEvaluators.addAll(ids);
      this.state = DriverRestartState.RestartInProgress;
    } else {
      final String errMsg = "Should not be setting the set of expected alive evaluators more than once.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }
  }

  /**
   * @return the IDs of the Evaluators from a previous Driver that have checked in with the Driver
   * in a restart situation.
   */
  public synchronized Set<String> getRecoveredEvaluatorIds() {
    return Collections.unmodifiableSet(this.previousEvaluators);
  }

  /**
   * Indicate that this Driver has re-established the connection with one more Evaluator of a previous run.
   * @return true if the driver restart is completed.
   */
  public synchronized boolean onRecoverEvaluatorIsRestartComplete(final String evaluatorId) {
    if (!this.previousEvaluators.contains(evaluatorId)) {
      final String errMsg = "Evaluator with evaluator ID " + evaluatorId + " not expected to be alive.";
      LOG.log(Level.SEVERE, errMsg);
      throw new DriverFatalRuntimeException(errMsg);
    }

    if (!this.recoveredEvaluators.add(evaluatorId)) {
      LOG.log(Level.WARNING, "Evaluator with evaluator ID " + evaluatorId + " added to the set" +
          " of recovered evaluators more than once. Ignoring second add...");
    }

    if (this.recoveredEvaluators.containsAll(this.previousEvaluators)) {
      this.state = DriverRestartState.RestartCompleted;
    }

    return this.state == DriverRestartState.RestartCompleted;
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
}
