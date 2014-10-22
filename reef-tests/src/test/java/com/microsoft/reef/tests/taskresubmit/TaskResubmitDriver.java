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
package com.microsoft.reef.tests.taskresubmit;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.tests.TestUtils;
import com.microsoft.reef.tests.fail.task.FailTaskCall;
import com.microsoft.reef.tests.library.exceptions.SimulatedTaskFailure;
import com.microsoft.reef.tests.library.exceptions.TaskSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
class TaskResubmitDriver {

  private static final Logger LOG = Logger.getLogger(TaskResubmitDriver.class.getName());

  private int failuresSeen = 0;

  @Inject
  TaskResubmitDriver() {
  }

  private static Configuration getTaskConfiguration() {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.TASK, FailTaskCall.class)
        .set(TaskConfiguration.IDENTIFIER, "FailTask")
        .build();
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      allocatedEvaluator.submitTask(getTaskConfiguration());
    }
  }

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {

      LOG.log(Level.INFO, "FailedTask: {0}", failedTask);

      final Throwable ex = failedTask.getReason().get();
      if (!TestUtils.hasCause(ex, SimulatedTaskFailure.class)) {
        final String msg = "Expected SimulatedTaskFailure from " + failedTask.getId();
        LOG.log(Level.SEVERE, msg, ex);
        throw new TaskSideFailure(msg, ex);
      }

      final ActiveContext activeContext = failedTask.getActiveContext().get();
      if (++TaskResubmitDriver.this.failuresSeen <= 1) { // resubmit the task
        activeContext.submitTask(getTaskConfiguration());
      } else { // Close the context
        activeContext.close();
      }
    }
  }
}
