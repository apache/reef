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

/**
 * The state that the evaluator is in in the driver restart process.
 */
@Private
@DriverSide
@Unstable
public enum EvaluatorRestartState {

  /**
   * The evaluator is not a restarted instance. Not expecting.
   */
  NOT_EXPECTED,

  /**
   * Have not yet heard back from an evaluator, but we are expecting it to report back.
   */
  EXPECTED,

  /**
   * Received the evaluator heartbeat, but have not yet processed it.
   */
  REPORTED,

  /**
   * The evaluator has had its recovery heartbeat processed.
   */
  REREGISTERED,

  /**
   * The evaluator has had its context/running task processed.
   */
  PROCESSED,

  /**
   * The evaluator has only contacted the driver after the expiration period.
   */
  EXPIRED,

  /**
   * The evaluator has failed on driver restart.
   */
  FAILED;

  /**
   * Check if the transition of {@link EvaluatorRestartState} from one state to another is legal.
   * @param fromState start state.
   * @param toState destination state.
   * @return true if the transition of {@link EvaluatorRestartState} is legal.
   * @deprecated TODO[JIRA REEF-1560] Use non-static method instead. Remove after version 0.16
   */
  public static boolean isLegalTransition(
      final EvaluatorRestartState fromState, final EvaluatorRestartState toState) {
    return fromState.isLegalTransition(toState);
  }

  /**
   * Check if the transition of {@link EvaluatorRestartState} from current state to the given one is legal.
   * @param toState destination state.
   * @return true if the transition is legal, false otherwise.
   */
  public final boolean isLegalTransition(final EvaluatorRestartState toState) {

    switch(this) {
    case EXPECTED:
      switch(toState) {
      case EXPIRED:
      case REPORTED:
        return true;
      default:
        return false;
      }

    case REPORTED:
      switch(toState) {
      case REREGISTERED:
        return true;
      default:
        return false;
      }

    case REREGISTERED:
      switch(toState) {
      case PROCESSED:
        return true;
      default:
        return false;
      }

    default:
      return false;
    }
  }

  /**
   * @return true if the evaluator has heartbeated back to the driver.
   */
  public boolean hasReported() {
    switch(this) {
    case REPORTED:
    case REREGISTERED:
    case PROCESSED:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if the evaluator has failed on driver restart or is not expected to report back to the driver.
   */
  public boolean isFailedOrNotExpected() {
    switch(this) {
    case FAILED:
    case NOT_EXPECTED:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if the evaluator has failed on driver restart or has been expired.
   */
  public boolean isFailedOrExpired() {
    switch(this) {
    case FAILED:
    case EXPIRED:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if the evaluator has had its recovery heartbeat processed.
   */
  public boolean isReregistered() {
    return this == REREGISTERED;
  }
}
