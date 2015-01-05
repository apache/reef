/**
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
package org.apache.reef.tests.taskcounting;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.tests.library.tasks.NoopTask;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Unit
final class TaskCountingDriver {

  private final Set<String> expectedRunningTaskIds = new HashSet<>();
  private AtomicInteger numberOfTaskSubmissions = new AtomicInteger(1000);

  @Inject
  TaskCountingDriver() {
  }

  private final Configuration getTaskConfiguration(final String taskId) {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, taskId)
        .set(TaskConfiguration.TASK, NoopTask.class)
        .build();
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (expectedRunningTaskIds) {
        final String taskId = "Task-" + numberOfTaskSubmissions.getAndDecrement();
        final Configuration taskConfiguration = getTaskConfiguration(taskId);
        allocatedEvaluator.submitTask(taskConfiguration);
        expectedRunningTaskIds.add(taskId);
      }
    }
  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      synchronized (expectedRunningTaskIds) {
        final boolean isExpected = expectedRunningTaskIds.remove(runningTask.getId());
        if (!isExpected) {
          throw new DriverSideFailure("Unexpected RunningTask: " + runningTask.getId());
        }
      }
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      synchronized (expectedRunningTaskIds) {
        final int nextTaskNumber = numberOfTaskSubmissions.getAndDecrement();
        if (nextTaskNumber > 0) {
          final String taskId = "Task-" + nextTaskNumber;
          completedTask.getActiveContext().submitTask(getTaskConfiguration(taskId));
          expectedRunningTaskIds.add(taskId);
        } else {
          completedTask.getActiveContext().close();
        }
      }
    }
  }

  final class DriverStopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (expectedRunningTaskIds) {
        if (!expectedRunningTaskIds.isEmpty()) {
          throw new DriverSideFailure("Still expecting RunningTasks");
        }
      }
    }
  }

}
