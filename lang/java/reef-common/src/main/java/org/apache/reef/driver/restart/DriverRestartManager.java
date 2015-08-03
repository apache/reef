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
import org.apache.reef.annotations.audience.RuntimeAuthor;

/**
 * Classes implementing this interface are in charge of recording evaluator
 * changes as they are allocated as well as recovering Evaluators and
 * discovering which evaluators are lost on the event of a driver restart.
 */
@DriverSide
@Private
@RuntimeAuthor
@Unstable
public interface DriverRestartManager {

  /**
   * Determines whether or not the driver has been restarted.
   */
  boolean isRestart();

  /**
   * This function has a few jobs crucial jobs to enable restart:
   * 1. Recover the list of evaluators that are reported to be alive by the Resource Manager.
   * 2. Make necessary operations to inform relevant runtime components about evaluators that are alive
   * with the set of evaluator IDs recovered in step 1.
   * 3. Make necessary operations to inform relevant runtime components about evaluators that have failed
   * during the driver restart period.
   */
  void onRestart();

  /**
   * Records the evaluators when it is allocated.
   * @param id The evaluator ID of the allocated evaluator.
   */
  void recordAllocatedEvaluator(final String id);


  /**
   * Records a removed evaluator into the evaluator log.
   * @param id The evaluator ID of the removed evaluator.
   */
  void recordRemovedEvaluator(final String id);
}
