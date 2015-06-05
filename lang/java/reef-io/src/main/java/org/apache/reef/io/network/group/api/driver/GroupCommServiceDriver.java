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
package org.apache.reef.io.network.group.api.driver;


import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.network.group.impl.driver.GroupCommDriverImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EStage;

@Private
@Provided
@DefaultImplementation(value = GroupCommDriverImpl.class)
public interface GroupCommServiceDriver extends GroupCommDriver {

  /**
   * Not user facing but used the Group Communication Service class.
   *
   * @return The running task stage that will handle the RunningTask
   * events
   */
  EStage<RunningTask> getGroupCommRunningTaskStage();

  /**
   * Not user facing but used the Group Communication Service class.
   *
   * @return The running task stage that will handle the FailedTask
   * events
   */
  EStage<FailedTask> getGroupCommFailedTaskStage();

  /**
   * Not user facing but used the Group Communication Service class.
   *
   * @return The running task stage that will handle the FailedEvaluator
   * events
   */
  EStage<FailedEvaluator> getGroupCommFailedEvaluatorStage();
}
