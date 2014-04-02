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

/**
 * Manages the evaluator state.
 */
@DriverSide
@Private
final class EvaluatorStateManager {
  private EvaluatorManager.State state = EvaluatorManager.State.ALLOCATED;

  @Inject
  private EvaluatorStateManager() {
  }

  synchronized void setRunning() {
    this.setState(EvaluatorManager.State.RUNNING);
  }

  synchronized void setSubmitted() {
    this.setState(EvaluatorManager.State.SUBMITTED);
  }

  synchronized void setDone() {
    this.setState(EvaluatorManager.State.DONE);
  }

  synchronized void setFailed() {
    this.setState(EvaluatorManager.State.FAILED);
  }

  synchronized void setKilled() {
    this.setState(EvaluatorManager.State.KILLED);
  }

  synchronized boolean isRunning() {
    return this.state.equals(EvaluatorManager.State.RUNNING);
  }

  synchronized boolean isDoneOrFailedOrKilled() {
    return (this.state == EvaluatorManager.State.DONE ||
        this.state == EvaluatorManager.State.FAILED ||
        this.state == EvaluatorManager.State.KILLED);
  }

  synchronized boolean isAllocatedOrSubmittedOrRunning() {
    return (this.state == EvaluatorManager.State.ALLOCATED ||
        this.state == EvaluatorManager.State.SUBMITTED ||
        this.state == EvaluatorManager.State.RUNNING);
  }

  synchronized boolean isSubmitted() {
    return EvaluatorManager.State.SUBMITTED == this.state;
  }

  synchronized boolean isAllocated() {
    return EvaluatorManager.State.ALLOCATED == this.state;
  }

  @Override
  public synchronized String toString() {
    return this.state.toString();
  }

  private synchronized void setState(final EvaluatorManager.State state) {
    if (!isLegal(this.state, state)) {
      throw new IllegalStateException("Illegal state transition from '" + this.state + "' to '" + state + "'");
    }
    this.state = state;

  }

  private static boolean isLegal(final EvaluatorManager.State from, final EvaluatorManager.State to) {
    // TODO
    return true;
  }
}
