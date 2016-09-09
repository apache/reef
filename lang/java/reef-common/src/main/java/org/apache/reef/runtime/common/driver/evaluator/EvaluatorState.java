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

/**
 * Various states that the EvaluatorManager could be in.
 * The EvaluatorManager is created when a resource has been allocated by the ResourceManager.
 */
@DriverSide
@Private
enum EvaluatorState {

  /** Initial state. */
  ALLOCATED,

  /** Client called AllocatedEvaluator.submitTask() and we're waiting for first contact. */
  SUBMITTED,

  /** First contact received, all communication channels established, Evaluator sent to client. */
  RUNNING,

  /** Evaluator is asked to shut down, but has not closed yet. */
  CLOSING,

  /** Clean shutdown. */
  DONE,

  /** Some failure occurred. */
  FAILED,

  /** Unclean shutdown. */
  KILLED;

  /**
   * Check if evaluator is in the initial state (ALLOCATED).
   * @return true if allocated, false otherwise.
   */
  public final boolean isAllocated() {
    return this == ALLOCATED;
  }

  /**
   * Check if evaluator is in SUBMITTED state.
   * @return true if submitted, false otherwise.
   */
  public final boolean isSubmitted() {
    return this == SUBMITTED;
  }

  /**
   * Check if the evaluator is in running state.
   * @return true if RUNNING, false otherwise.
   */
  public final boolean isRunning() {
    return this == RUNNING;
  }

  /**
   * Check if the evaluator is in the process of being shut down.
   * @return true if evaluator is being closed, false otherwise.
   */
  public final boolean isClosing() {
    return this == CLOSING;
  }

  /**
   * Check if evaluator is in one of the active states (ALLOCATED, SUBMITTED, or RUNNING).
   * @return true if evaluator is available, false if it is closed or in the process of being shut down.
   */
  public final boolean isAvailable() {
    return this == ALLOCATED || this == SUBMITTED || this == RUNNING;
  }

  /**
   * Check if the evaluator is stopped. That is, in one of the DONE, FAILED, or KILLED states.
   * @return true if evaluator completed, false if it is still available or in the process of being shut down.
   */
  public final boolean isCompleted() {
    return this == DONE || this == FAILED || this == KILLED;
  }

  /**
   * Check if the evaluator is closed due to an error. That is, in FAILED or KILLED state.
   * @return true if evaluator is stopped due to an error, true otherwise.
   */
  public final boolean isCompletedAbnormally() {
    return this == FAILED || this == KILLED;
  }

  /**
   * Check if transition from current state to the given one is legal.
   * @param toState new state to transition to.
   * @return true if transition is legal, false otherwise.
   */
  public final boolean isLegalTransition(final EvaluatorState toState) {

    if (this == toState) {
      return true;
    }

    switch(this) {

    case ALLOCATED:
      switch(toState) {
      case SUBMITTED:
      case CLOSING:
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    case SUBMITTED:
      switch(toState) {
      case RUNNING:
      case CLOSING:
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    case RUNNING:
      switch(toState) {
      case CLOSING:
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    case CLOSING:
      switch(toState) {
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    default:
      return false;
    }
  }
}
