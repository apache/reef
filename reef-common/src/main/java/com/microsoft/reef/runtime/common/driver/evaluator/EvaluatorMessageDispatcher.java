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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.runtime.common.driver.DispatchingEStage;
import com.microsoft.reef.runtime.common.driver.DriverExceptionHandler;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;

/**
 * Central dispatcher for all Evaluator related events. This exists once per Evaluator.
 */
final class EvaluatorMessageDispatcher {
  /**
   * The actual dispatcher used.
   */
  private final DispatchingEStage dispatcher;

  @Inject
  EvaluatorMessageDispatcher(final @Parameter(DriverConfigurationOptions.ActiveContextHandlers.class) Set<EventHandler<ActiveContext>> activeContextEventHandlers,
                             final @Parameter(DriverConfigurationOptions.ClosedContextHandlers.class) Set<EventHandler<ClosedContext>> closedContextEventHandlers,
                             final @Parameter(DriverConfigurationOptions.FailedContextHandlers.class) Set<EventHandler<FailedContext>> failedContextEventHandlers,
                             final @Parameter(DriverConfigurationOptions.ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
                             final @Parameter(DriverConfigurationOptions.RunningTaskHandlers.class) Set<EventHandler<RunningTask>> runningTaskEventHandlers,
                             final @Parameter(DriverConfigurationOptions.CompletedTaskHandlers.class) Set<EventHandler<CompletedTask>> completedTaskEventHandlers,
                             final @Parameter(DriverConfigurationOptions.SuspendedTaskHandlers.class) Set<EventHandler<SuspendedTask>> suspendedTaskEventHandlers,
                             final @Parameter(DriverConfigurationOptions.TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
                             final @Parameter(DriverConfigurationOptions.FailedTaskHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
                             final @Parameter(DriverConfigurationOptions.AllocatedEvaluatorHandlers.class) Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorEventHandlers,
                             final @Parameter(DriverConfigurationOptions.FailedEvaluatorHandlers.class) Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
                             final @Parameter(DriverConfigurationOptions.CompletedEvaluatorHandlers.class) Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers,
                             final @Parameter(DriverConfigurationOptions.EvaluatorDispatcherThreads.class) int numberOfThreads,
                             final DriverExceptionHandler driverExceptionHandler) {
    this.dispatcher = new DispatchingEStage(driverExceptionHandler, numberOfThreads);

    this.dispatcher.register(ActiveContext.class, activeContextEventHandlers);
    this.dispatcher.register(ClosedContext.class, closedContextEventHandlers);
    this.dispatcher.register(FailedContext.class, failedContextEventHandlers);
    this.dispatcher.register(ContextMessage.class, contextMessageHandlers);

    this.dispatcher.register(RunningTask.class, runningTaskEventHandlers);
    this.dispatcher.register(CompletedTask.class, completedTaskEventHandlers);
    this.dispatcher.register(SuspendedTask.class, suspendedTaskEventHandlers);
    this.dispatcher.register(TaskMessage.class, taskMessageEventHandlers);
    this.dispatcher.register(FailedTask.class, taskExceptionEventHandlers);

    this.dispatcher.register(FailedEvaluator.class, failedEvaluatorHandlers);
    this.dispatcher.register(CompletedEvaluator.class, completedEvaluatorHandlers);
    this.dispatcher.register(AllocatedEvaluator.class, allocatedEvaluatorEventHandlers);
  }

  void onEvaluatorAllocated(final AllocatedEvaluator allocatedEvaluator) {
    this.dispatcher.onNext(AllocatedEvaluator.class, allocatedEvaluator);
  }

  void onEvaluatorFailed(final FailedEvaluator failedEvaluator) {
    this.dispatcher.onNext(FailedEvaluator.class, failedEvaluator);
  }

  void onEvaluatorCompleted(final CompletedEvaluator completedEvaluator) {
    this.dispatcher.onNext(CompletedEvaluator.class, completedEvaluator);
  }

  void onTaskRunning(final RunningTask runningTask) {
    this.dispatcher.onNext(RunningTask.class, runningTask);
  }

  void onTaskCompleted(final CompletedTask completedTask) {
    this.dispatcher.onNext(CompletedTask.class, completedTask);
  }

  void onTaskSuspended(final SuspendedTask suspendedTask) {
    this.dispatcher.onNext(SuspendedTask.class, suspendedTask);
  }

  void onTaskMessage(final TaskMessage taskMessage) {
    this.dispatcher.onNext(TaskMessage.class, taskMessage);
  }

  void onTaskFailed(final FailedTask failedTask) {
    this.dispatcher.onNext(FailedTask.class, failedTask);
  }

  void onContextActive(final ActiveContext activeContext) {
    this.dispatcher.onNext(ActiveContext.class, activeContext);
  }

  public void onContextClose(final ClosedContext closedContext) {
    this.dispatcher.onNext(ClosedContext.class, closedContext);
  }

  void onContextFailed(final FailedContext failedContext) {
    this.dispatcher.onNext(FailedContext.class, failedContext);
  }

  void onContextMessage(final ContextMessage contextMessage) {
    this.dispatcher.onNext(ContextMessage.class, contextMessage);
  }

  boolean isEmpty() {
    return this.dispatcher.isEmpty();
  }
}
