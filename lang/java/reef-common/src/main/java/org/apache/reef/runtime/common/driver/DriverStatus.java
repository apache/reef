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
package org.apache.reef.runtime.common.driver;

/**
 * The status of the Driver.
 */
public enum DriverStatus {

  PRE_INIT,
  INIT,
  RUNNING,
  SHUTTING_DOWN,
  FAILING;

  /**
   * Check if the driver is in process of shutting down (either gracefully or due to an error).
   * @return true if the driver is shutting down (gracefully or otherwise).
   */
  public boolean isClosing() {
    return this == SHUTTING_DOWN || this == FAILING;
  }

  /**
   * Check whether a driver state transition from current state to a given one is legal.
   * @param toStatus Destination state.
   * @return true if transition is valid, false otherwise.
   */
  public boolean isLegalTransition(final DriverStatus toStatus) {

    switch (this) {

    case PRE_INIT:
      switch (toStatus) {
      case INIT:
        return true;
      default:
        return false;
      }

    case INIT:
      switch (toStatus) {
      case RUNNING:
        return true;
      default:
        return false;
      }

    case RUNNING:
      switch (toStatus) {
      case SHUTTING_DOWN:
      case FAILING:
        return true;
      default:
        return false;
      }

    default:
      return false;
    }
  }
}
