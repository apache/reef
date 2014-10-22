/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;

import javax.inject.Inject;
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
  private EvaluatorState state = EvaluatorState.ALLOCATED;

  @Inject
  private EvaluatorStatusManager() {
    LOG.log(Level.FINE, "Instantiated 'EvaluatorStatusManager'");
  }

  synchronized void setRunning() {
    this.setState(EvaluatorState.RUNNING);
  }

  synchronized void setSubmitted() {
    this.setState(EvaluatorState.SUBMITTED);
  }

  synchronized void setDone() {
    this.setState(EvaluatorState.DONE);
  }

  synchronized void setFailed() {
    this.setState(EvaluatorState.FAILED);
  }

  synchronized void setKilled() {
    this.setState(EvaluatorState.KILLED);
  }

  synchronized boolean isRunning() {
    return this.state.equals(EvaluatorState.RUNNING);
  }

  synchronized boolean isDoneOrFailedOrKilled() {
    return (this.state == EvaluatorState.DONE ||
        this.state == EvaluatorState.FAILED ||
        this.state == EvaluatorState.KILLED);
  }

  synchronized boolean isAllocatedOrSubmittedOrRunning() {
    return (this.state == EvaluatorState.ALLOCATED ||
        this.state == EvaluatorState.SUBMITTED ||
        this.state == EvaluatorState.RUNNING);
  }

  synchronized boolean isSubmitted() {
    return EvaluatorState.SUBMITTED == this.state;
  }

  synchronized boolean isAllocated() {
    return EvaluatorState.ALLOCATED == this.state;
  }

  @Override
  public synchronized String toString() {
    return this.state.toString();
  }

  private synchronized void setState(final EvaluatorState state) {
    if (!isLegal(this.state, state)) {
      throw new IllegalStateException("Illegal state transition from '" + this.state + "' to '" + state + "'");
    }
    this.state = state;

  }

  private static boolean isLegal(final EvaluatorState from, final EvaluatorState to) {
    // TODO
    return true;
  }
}
