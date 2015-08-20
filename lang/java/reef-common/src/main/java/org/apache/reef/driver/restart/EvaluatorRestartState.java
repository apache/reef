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
  NOT_RESTARTED_EVALUATOR,

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
   * The evaluator has had its running task processed.
   */
  TASK_RUNNING_FIRED,

  /**
   * The evaluator has only contacted the driver after the expiration period.
   */
  EXPIRED;

  /**
   * @return true if the evaluator has heartbeated back to the driver.
   */
  public boolean hasReported() {
    switch(this) {
    case REPORTED:
    case REREGISTERED:
    case TASK_RUNNING_FIRED:
      return true;
    default:
      return false;
    }
  }
}