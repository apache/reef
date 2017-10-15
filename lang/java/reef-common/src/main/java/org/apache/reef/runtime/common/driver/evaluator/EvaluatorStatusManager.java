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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages Status of a single Evaluator.
 */
@DriverSide
@Private
final class EvaluatorStatusManager {

  private static final Logger LOG = Logger.getLogger(EvaluatorStatusManager.class.getName());

  /**
   * The state managed.
   */
  private final AtomicReference<EvaluatorState> state = new AtomicReference<>(EvaluatorState.ALLOCATED);

  @Inject
  private EvaluatorStatusManager() {
    LOG.log(Level.FINE, "Instantiated 'EvaluatorStatusManager'");
  }

  void setRunning() {
    this.setState(EvaluatorState.RUNNING);
  }

  void setSubmitted() {
    this.setState(EvaluatorState.SUBMITTED);
  }

  void setClosing() {
    this.setState(EvaluatorState.CLOSING);
  }

  void setDone() {
    this.setState(EvaluatorState.DONE);
  }

  void setFailed() {
    this.setState(EvaluatorState.FAILED);
  }

  void setKilled() {
    this.setState(EvaluatorState.KILLED);
  }

  /**
   * Check if evaluator is in the initial state (ALLOCATED).
   * @return true if allocated, false otherwise.
   */
  boolean isAllocated() {
    return this.state.get().isAllocated();
  }

  /**
   * Check if evaluator is in SUBMITTED state.
   * @return true if submitted, false otherwise.
   */
  boolean isSubmitted() {
    return this.state.get().isSubmitted();
  }

  /**
   * Check if the evaluator is in running state.
   * @return true if RUNNING, false otherwise.
   */
  boolean isRunning() {
    return this.state.get().isRunning();
  }

  /**
   * Check if the evaluator is in the process of being shut down.
   * @return true if evaluator is being closed, false otherwise.
   */
  boolean isClosing() {
    return this.state.get().isClosing();
  }

  /**
   * Check if evaluator is in one of the active states (ALLOCATED, SUBMITTED, or RUNNING).
   * @return true if evaluator is available, false if it is closed or in the process of being shut down.
   * @deprecated TODO[JIRA REEF-1560] Use isAvailable() method instead. Remove after version 0.16
   */
  @Deprecated
  boolean isAllocatedOrSubmittedOrRunning() {
    return this.state.get().isAvailable();
  }

  /**
   * Check if evaluator is in one of the active states (ALLOCATED, SUBMITTED, or RUNNING).
   * @return true if evaluator is available, false if it is closed or in the process of being shut down.
   */
  boolean isAvailable() {
    return this.state.get().isAvailable();
  }

  /**
   * Check if the evaluator is stopped. That is, in one of the DONE, FAILED or KILLED states.
   * @return true if evaluator completed, false if it is still available or in the process of being shut down.
   * @deprecated TODO[JIRA REEF-1560] Use isCompleted() method instead. Remove after version 0.16
   */
  @Deprecated
  boolean isDoneOrFailedOrKilled() {
    return this.state.get().isCompleted();
  }

  /**
   * Check if the evaluator is stopped. That is, in one of the DONE, FAILED, PREEMPTED or KILLED states.
   * @return true if evaluator completed, false if it is still available or in the process of being shut down.
   */
  boolean isCompleted() {
    return this.state.get().isCompleted();
  }

  /**
   * Check if the evaluator is closed due to an error. That is, in FAILED or KILLED state.
   * @return true if evaluator is stopped due to an error, true otherwise.
   * @deprecated TODO[JIRA REEF-1560] Use isCompletedAbnormally() method instead. Remove after version 0.16
   */
  @Deprecated
  boolean isFailedOrKilled() {
    return this.state.get().isCompletedAbnormally();
  }

  /**
   * Check if the evaluator is closed due to an error. That is, in FAILED or KILLED state.
   * @return true if evaluator is stopped due to an error, true otherwise.
   */
  boolean isCompletedAbnormally() {
    return this.state.get().isCompletedAbnormally();
  }

  /**
   * Return string representation of the current state of hte Evaluator, like RUNNING or DONE.
   * @return string representation of the current state of the Evaluator.
   */
  @Override
  public String toString() {
    return this.state.get().toString();
  }

  /**
   * Transition to the new state of the evaluator, if possible.
   * @param toState New state of the evaluator.
   * @throws IllegalStateException if state transition is not valid.
   */
  private void setState(final EvaluatorState toState) {
    while (true) {

      final EvaluatorState fromState = this.state.get();
      if (fromState == toState) {
        break;
      }

      if (!fromState.isLegalTransition(toState)) {
        LOG.log(Level.WARNING, "Illegal state transition: {0} -> {1}", new Object[] {fromState, toState});
        throw new IllegalStateException("Illegal state transition: " + fromState + " -> " + toState);
      }

      if (this.state.compareAndSet(fromState, toState)) {
        break;
      }
    }
  }
}
