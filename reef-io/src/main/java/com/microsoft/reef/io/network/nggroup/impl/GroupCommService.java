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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.EvaluatorDispatcherThreads;
import com.microsoft.reef.driver.parameters.ServiceEvaluatorFailedHandlers;
import com.microsoft.reef.driver.parameters.ServiceTaskFailedHandlers;
import com.microsoft.reef.driver.parameters.TaskRunningHandlers;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

@Unit
public class GroupCommService {

  private final GroupCommDriver groupCommDriver;

  @Inject
  public GroupCommService(final GroupCommDriver groupCommDriver) {
    this.groupCommDriver = groupCommDriver;
  }

  public static Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(TaskRunningHandlers.class, RunningTaskHandler.class)
        .bindSetEntry(ServiceTaskFailedHandlers.class, FailedTaskHandler.class)
        .bindSetEntry(ServiceEvaluatorFailedHandlers.class, FailedEvaluatorHandler.class)
        .bindNamedParameter(EvaluatorDispatcherThreads.class, "1")
        .build();
  }

  public class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      groupCommDriver.getGroupCommFailedEvaluatorStage().onNext(failedEvaluator);
    }
  }

  public class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      groupCommDriver.getGroupCommRunningTaskStage().onNext(runningTask);
    }
  }

  public class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      groupCommDriver.getGroupCommFailedTaskStage().onNext(failedTask);
    }
  }
}
