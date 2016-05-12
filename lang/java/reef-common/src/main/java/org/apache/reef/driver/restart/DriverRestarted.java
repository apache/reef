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
import org.apache.reef.wake.time.event.StartTime;

import java.util.Set;

/**
 * Am event encapsulating the time of Driver restart as well as
 * the set of Evaluator IDs of Evaluators that are expected to
 * report back to the Driver after restart.
 */
@Public
@Provided
@Unstable
public interface DriverRestarted {
  /**
   * @return The number of times the Driver has been resubmitted. Not including the initial attempt.
   */
  int getResubmissionAttempts();

  /**
   * @return The time of restart.
   */
  StartTime getStartTime();

  /**
   * @return The set of Evaluator IDs of Evaluators that are expected
   * to report back to the Driver after restart.
   */
  Set<String> getExpectedEvaluatorIds();
}
