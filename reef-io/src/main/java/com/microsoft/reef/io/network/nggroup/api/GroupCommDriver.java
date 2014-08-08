/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.network.nggroup.impl.GroupCommDriverImpl;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;

/**
 *
 */
@DefaultImplementation(value = GroupCommDriverImpl.class)
public interface GroupCommDriver {

  /**
   * @param numberOfTasks
   * @param string
   * @return
   */
  CommunicationGroupDriver newCommunicationGroup(Class<? extends Name<String>> groupName, int numberOfTasks);

  /**
   * @param activeContext
   * @return
   */
  boolean configured(ActiveContext activeContext);


  /**
   * @param activeContext
   * @return
   */
  Configuration getContextConf();

  /**
   * @param activeContext
   * @return
   */
  Configuration getServiceConf();

  /**
   * @param partialTaskConf
   * @return
   */
  Configuration getTaskConfiguration(Configuration partialTaskConf);

  /**
   * @return
   */
  EStage<RunningTask> getGroupCommRunningTaskStage();

  /**
   * @return
   */
  EStage<FailedTask> getGroupCommFailedTaskStage();

  /**
   * @return
   */
  EStage<FailedEvaluator> getGroupCommFailedEvaluatorStage();

}
