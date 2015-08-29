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

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.wake.time.Time;

/**
 * The object used to notify the user that the Driver Restart process has been completed.
 */
@Public
@Unstable
@Provided
public interface DriverRestartCompleted {

  /**
   * @return the completed time of Driver restart.
   */
  Time getCompletedTime();

  /**
   * @return True if Driver restart completion was triggered by a timeout. False if triggered due to all Evaluators
   * reporting back.
   */
  boolean isTimedOut();
}
