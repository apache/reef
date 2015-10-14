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
package org.apache.reef.tests.watcher;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.SuspendedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@Unit
public final class WatcherTestDriver {

  private static final String ROOT_CONTEXT_ID = "ROOT_CONTEXT";
  private static final String FIRST_CONTEXT_ID = "FIRST_CONTEXT";

  private final EvaluatorRequestor evaluatorRequestor;
  private final TestEventStream testEventStream;

  /**
   * The first evaluator will be failed to generate FailedEvaluator.
   */
  private final AtomicBoolean isFirstEvaluator;

  /**
   * The first task will be suspended to generate SuspendedTask.
   */
  private final AtomicBoolean isFirstTask;

  @Inject
  private WatcherTestDriver(final EvaluatorRequestor evaluatorRequestor,
                            final TestEventStream testEventStream) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.testEventStream = testEventStream;
    this.isFirstEvaluator = new AtomicBoolean(true);
    this.isFirstTask = new AtomicBoolean(true);
  }

  public final class DriverStartedHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      evaluatorRequestor.submit(EvaluatorRequest
          .newBuilder()
          .setMemory(64)
          .setNumberOfCores(1)
          .setNumber(2)
          .build());
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      if (isFirstEvaluator.compareAndSet(true, false)) {
        allocatedEvaluator.submitContext(getFailedContextConfiguration());
      } else {
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, ROOT_CONTEXT_ID)
            .build());
      }
    }
  }

  public final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // no-op
    }
  }

  public final class ContextActivatedHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      if (activeContext.getId().equals(ROOT_CONTEXT_ID)) {
        activeContext.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, FIRST_CONTEXT_ID)
            .build());
      } else if (activeContext.getId().equals(FIRST_CONTEXT_ID)) {
        activeContext.submitContext(getFailedContextConfiguration());
      }
    }
  }

  public final class ContextFailedHandler implements EventHandler<FailedContext> {

    @Override
    public void onNext(final FailedContext failedContext) {
      failedContext.getParentContext().get().submitTask(getFailedTaskConfiguration());
    }
  }

  public final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      if (isFirstTask.compareAndSet(true, false)) {
        runningTask.suspend();
      }
    }
  }

  public final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      failedTask.getActiveContext().get().submitTask(getTaskConfiguration(true));
    }
  }

  public final class TaskSuspendedHandler implements EventHandler<SuspendedTask> {

    @Override
    public void onNext(final SuspendedTask value) {
      value.getActiveContext().submitTask(getTaskConfiguration(false));
    }
  }

  public final class RuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public void onNext(final RuntimeStop runtimeStop) {
      testEventStream.validate();
    }
  }

  private Configuration getTaskConfiguration(final boolean isTaskSuspended) {
    final Configuration taskConf = TaskConfiguration.CONF
        .set(TaskConfiguration.TASK, WatcherTestTask.class)
        .set(TaskConfiguration.IDENTIFIER, "TASK")
        .set(TaskConfiguration.ON_SEND_MESSAGE, WatcherTestTask.class)
        .set(TaskConfiguration.ON_SUSPEND, WatcherTestTask.TaskSuspendedHandler.class)
        .build();

    return Tang.Factory.getTang().newConfigurationBuilder(taskConf)
        .bindNamedParameter(IsTaskSuspended.class, String.valueOf(isTaskSuspended))
        .build();
  }

  private Configuration getFailedTaskConfiguration() {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.TASK, WatcherTestTask.class)
        .set(TaskConfiguration.IDENTIFIER, "FAILED_TASK")
        .set(TaskConfiguration.ON_TASK_STARTED, FailedTaskStartHandler.class)
        .build();
  }

  private Configuration getFailedContextConfiguration() {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "FAILED_CONTEXT")
        .set(ContextConfiguration.ON_CONTEXT_STARTED, FailedContextHandler.class)
        .build();
  }
}
