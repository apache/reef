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

package org.apache.reef.runtime.common.driver.evaluator.pojos;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.ReefServiceProtos;

/**
 * DriverSide representation of ReefServiceProtos.State.
 */
@DriverSide
@Private
public enum State {

  INIT,
  RUNNING,
  SUSPEND,
  DONE,
  FAILED,
  KILLED;

  /**
   * Get a driver-side state given the proto. It is a 1:1 mapping.
   * @param protoState remote state from the proto.
   * @return a corresponding (identical) driver-side state (always a 1:1 mapping).
   */
  public static State fromProto(final ReefServiceProtos.State protoState) {
    switch (protoState) {
    case INIT:
      return INIT;
    case RUNNING:
      return RUNNING;
    case SUSPEND:
      return SUSPEND;
    case DONE:
      return DONE;
    case FAILED:
      return FAILED;
    case KILLED:
      return KILLED;
    default:
      throw new IllegalStateException("Unknown state " + protoState + " in EvaluatorStatusProto");
    }
  }

  /**
   * Checks if the ResourceManager can switch from the current state to the target state.
   * See REEF-826 for the state transition matrix.
   * @param toState state to switch to.
   * @return true if the transition is legal; false otherwise.
   */
  public final boolean isLegalTransition(final State toState) {

    if (this == toState) {
      return true;
    }

    switch (this) {

    case INIT:
      switch (toState) {
      case RUNNING:
      case SUSPEND:
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    case RUNNING:
      switch (toState) {
      case SUSPEND:
      case DONE:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
      }

    case SUSPEND:
      switch (toState) {
      case RUNNING:
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

  /**
   * Check if container is in RUNNING state.
   * @return true if container is running.
   */
  public final boolean isRunning() {
    return this == RUNNING;
  }


  /**
   * Check if container is available - that is, in one of the states INIT, RUNNING, or SUSPEND.
   * @return true if container is available, false if it is closed or in the process of being shut down.
   */
  public final boolean isAvailable() {
    return this == INIT || this == RUNNING || this == SUSPEND;
  }

  /**
   * Check if the container is stopped. That is, in one of the DONE, FAILED, or KILLED states.
   * @return true if the container is completed, false if it is still available or suspended.
   */
  public final boolean isCompleted() {
    return this == DONE || this == FAILED || this == KILLED;
  }

  /**
   * Check if the container is can be restarted. That is, in one of the INIT, RUNNING, or FAILED states.
   * @return true if the container can be restarted.
   */
  public final boolean isRestartable() {
    return this == INIT || this == RUNNING || this == FAILED;
  }
}
