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
 * Represents the current driver restart progress.
 */
@Private
@DriverSide
@Unstable
public enum DriverRestartState {
  /**
   *  Driver has not begun the restart progress yet.
   */
  NOT_RESTARTED,

  /**
   * Driver has been notified of the restart by the runtime, but has not yet
   * received its set of evaluator IDs to recover yet.
   */
  BEGAN,

  /**
   * Driver has received its set of evaluator IDs to recover.
   */
  IN_PROGRESS,

  /**
   * Driver has recovered all the evaluator IDs that it can, and the restart process is completed.
   */
  COMPLETED;

  /**
   * @return  true if the restart is in process.
   */
  public boolean isRestarting() {
    switch (this) {
    case BEGAN:
    case IN_PROGRESS:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if the driver began the restart process. Can be already done with the restart process.
   */
  public boolean hasRestarted() {
    return this != NOT_RESTARTED;
  }

  /**
   * the negation of {@link #hasRestarted()}.
   */
  public boolean hasNotRestarted() {
    return !this.hasRestarted();
  }
}
