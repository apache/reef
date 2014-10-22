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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.*;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.runtime.common.DriverRestartCompleted;
import com.microsoft.reef.runtime.common.driver.DriverExceptionHandler;
import com.microsoft.reef.runtime.common.utils.DispatchingEStage;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central dispatcher for all Evaluator related events. This exists once per Evaluator.
 */
public final class EvaluatorMessageDispatcher {

  private static final Logger LOG = Logger.getLogger(EvaluatorMessageDispatcher.class.getName());

  /**
   * Dispatcher used for application provided event handlers.
   */
  private final DispatchingEStage applicationDispatcher;

  /**
   * Dispatcher used for service provided event handlers.
   */
  private final DispatchingEStage serviceDispatcher;


  /**
   * Dispatcher used for application provided driver-restart specific event handlers.
   */
  private final DispatchingEStage driverRestartApplicationDispatcher;

  /**
   * Dispatcher used for service provided driver-restart specific event handlers.
   */
  private final DispatchingEStage driverRestartServiceDispatcher;

  @Inject
  EvaluatorMessageDispatcher(
      // Application-provided Context event handlers
      final @Parameter(ContextActiveHandlers.class) Set<EventHandler<ActiveContext>> contextActiveHandlers,
      final @Parameter(ContextClosedHandlers.class) Set<EventHandler<ClosedContext>> contextClosedHandlers,
      final @Parameter(ContextFailedHandlers.class) Set<EventHandler<FailedContext>> contextFailedHandlers,
      final @Parameter(ContextMessageHandlers.class) Set<EventHandler<ContextMessage>> contextMessageHandlers,
      // Service-provided Context event handlers
      final @Parameter(ServiceContextActiveHandlers.class) Set<EventHandler<ActiveContext>> serviceContextActiveHandlers,
      final @Parameter(ServiceContextClosedHandlers.class) Set<EventHandler<ClosedContext>> serviceContextClosedHandlers,
      final @Parameter(ServiceContextFailedHandlers.class) Set<EventHandler<FailedContext>> serviceContextFailedHandlers,
      final @Parameter(ServiceContextMessageHandlers.class) Set<EventHandler<ContextMessage>> serviceContextMessageHandlers,
      // Application-provided Task event handlers
      final @Parameter(TaskRunningHandlers.class) Set<EventHandler<RunningTask>> taskRunningHandlers,
      final @Parameter(TaskCompletedHandlers.class) Set<EventHandler<CompletedTask>> taskCompletedHandlers,
      final @Parameter(TaskSuspendedHandlers.class) Set<EventHandler<SuspendedTask>> taskSuspendedHandlers,
      final @Parameter(TaskMessageHandlers.class) Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      final @Parameter(TaskFailedHandlers.class) Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      // Service-provided Task event handlers
      final @Parameter(ServiceTaskRunningHandlers.class) Set<EventHandler<RunningTask>> serviceTaskRunningEventHandlers,
      final @Parameter(ServiceTaskCompletedHandlers.class) Set<EventHandler<CompletedTask>> serviceTaskCompletedEventHandlers,
      final @Parameter(ServiceTaskSuspendedHandlers.class) Set<EventHandler<SuspendedTask>> serviceTaskSuspendedEventHandlers,
      final @Parameter(ServiceTaskMessageHandlers.class) Set<EventHandler<TaskMessage>> serviceTaskMessageEventHandlers,
      final @Parameter(ServiceTaskFailedHandlers.class) Set<EventHandler<FailedTask>> serviceTaskExceptionEventHandlers,
      // Application-provided Evaluator event handlers
      final @Parameter(EvaluatorAllocatedHandlers.class) Set<EventHandler<AllocatedEvaluator>> evaluatorAllocatedHandlers,
      final @Parameter(EvaluatorFailedHandlers.class) Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers,
      final @Parameter(EvaluatorCompletedHandlers.class) Set<EventHandler<CompletedEvaluator>> evaluatorCompletedHandlers,
      // Service-provided Evaluator event handlers
      final @Parameter(ServiceEvaluatorAllocatedHandlers.class) Set<EventHandler<AllocatedEvaluator>> serviceEvaluatorAllocatedEventHandlers,
      final @Parameter(ServiceEvaluatorFailedHandlers.class) Set<EventHandler<FailedEvaluator>> serviceEvaluatorFailedHandlers,
      final @Parameter(ServiceEvaluatorCompletedHandlers.class) Set<EventHandler<CompletedEvaluator>> serviceEvaluatorCompletedHandlers,

      // Application event handlers specific to a Driver restart
      final @Parameter(DriverRestartTaskRunningHandlers.class) Set<EventHandler<RunningTask>> driverRestartTaskRunningHandlers,
      final @Parameter(DriverRestartContextActiveHandlers.class) Set<EventHandler<ActiveContext>> driverRestartActiveContextHandlers,
      final @Parameter(DriverRestartCompletedHandlers.class) Set<EventHandler<DriverRestartCompleted>> driverRestartCompletedHandlers,

      // Service-provided event handlers specific to a Driver restart
      final @Parameter(ServiceDriverRestartTaskRunningHandlers.class) Set<EventHandler<RunningTask>> serviceDriverRestartTaskRunningHandlers,
      final @Parameter(ServiceDriverRestartContextActiveHandlers.class) Set<EventHandler<ActiveContext>> serviceDriverRestartActiveContextHandlers,
      final @Parameter(ServiceDriverRestartCompletedHandlers.class) Set<EventHandler<DriverRestartCompleted>> serviceDriverRestartCompletedHandlers,

      final @Parameter(EvaluatorDispatcherThreads.class) int numberOfThreads,
      final @Parameter(EvaluatorManager.EvaluatorIdentifier.class) String evaluatorIdentifier,
      final DriverExceptionHandler driverExceptionHandler) {

    this.serviceDispatcher = new DispatchingEStage(driverExceptionHandler, numberOfThreads, evaluatorIdentifier);
    this.applicationDispatcher = new DispatchingEStage(this.serviceDispatcher);
    this.driverRestartApplicationDispatcher = new DispatchingEStage(this.serviceDispatcher);
    this.driverRestartServiceDispatcher = new DispatchingEStage(this.serviceDispatcher);

    { // Application Context event handlers
      this.applicationDispatcher.register(ActiveContext.class, contextActiveHandlers);
      this.applicationDispatcher.register(ClosedContext.class, contextClosedHandlers);
      this.applicationDispatcher.register(FailedContext.class, contextFailedHandlers);
      this.applicationDispatcher.register(ContextMessage.class, contextMessageHandlers);
    }
    { // Service Context event handlers
      this.serviceDispatcher.register(ActiveContext.class, serviceContextActiveHandlers);
      this.serviceDispatcher.register(ClosedContext.class, serviceContextClosedHandlers);
      this.serviceDispatcher.register(FailedContext.class, serviceContextFailedHandlers);
      this.serviceDispatcher.register(ContextMessage.class, serviceContextMessageHandlers);
    }
    { // Application Task event handlers.
      this.applicationDispatcher.register(RunningTask.class, taskRunningHandlers);
      this.applicationDispatcher.register(CompletedTask.class, taskCompletedHandlers);
      this.applicationDispatcher.register(SuspendedTask.class, taskSuspendedHandlers);
      this.applicationDispatcher.register(TaskMessage.class, taskMessageEventHandlers);
      this.applicationDispatcher.register(FailedTask.class, taskExceptionEventHandlers);
    }
    { // Service Task event handlers
      this.serviceDispatcher.register(RunningTask.class, serviceTaskRunningEventHandlers);
      this.serviceDispatcher.register(CompletedTask.class, serviceTaskCompletedEventHandlers);
      this.serviceDispatcher.register(SuspendedTask.class, serviceTaskSuspendedEventHandlers);
      this.serviceDispatcher.register(TaskMessage.class, serviceTaskMessageEventHandlers);
      this.serviceDispatcher.register(FailedTask.class, serviceTaskExceptionEventHandlers);
    }
    { // Application Evaluator event handlers
      this.applicationDispatcher.register(FailedEvaluator.class, evaluatorFailedHandlers);
      this.applicationDispatcher.register(CompletedEvaluator.class, evaluatorCompletedHandlers);
      this.applicationDispatcher.register(AllocatedEvaluator.class, evaluatorAllocatedHandlers);
    }
    { // Service Evaluator event handlers
      this.serviceDispatcher.register(FailedEvaluator.class, serviceEvaluatorFailedHandlers);
      this.serviceDispatcher.register(CompletedEvaluator.class, serviceEvaluatorCompletedHandlers);
      this.serviceDispatcher.register(AllocatedEvaluator.class, serviceEvaluatorAllocatedEventHandlers);
    }

    // Application event handlers specific to a Driver restart
    {
      this.driverRestartApplicationDispatcher.register(RunningTask.class, driverRestartTaskRunningHandlers);
      this.driverRestartApplicationDispatcher.register(ActiveContext.class, driverRestartActiveContextHandlers);
      this.driverRestartApplicationDispatcher.register(DriverRestartCompleted.class, driverRestartCompletedHandlers);
    }

    // Service event handlers specific to a Driver restart
    {
      this.driverRestartServiceDispatcher.register(RunningTask.class, serviceDriverRestartTaskRunningHandlers);
      this.driverRestartServiceDispatcher.register(ActiveContext.class, serviceDriverRestartActiveContextHandlers);
      this.driverRestartServiceDispatcher.register(DriverRestartCompleted.class, serviceDriverRestartCompletedHandlers);
    }
    LOG.log(Level.FINE, "Instantiated 'EvaluatorMessageDispatcher'");
  }

  public void onEvaluatorAllocated(final AllocatedEvaluator allocatedEvaluator) {
    this.dispatch(AllocatedEvaluator.class, allocatedEvaluator);
  }

  public void onEvaluatorFailed(final FailedEvaluator failedEvaluator) {
    this.dispatch(FailedEvaluator.class, failedEvaluator);
  }

  public void onEvaluatorCompleted(final CompletedEvaluator completedEvaluator) {
    this.dispatch(CompletedEvaluator.class, completedEvaluator);
  }

  public void onTaskRunning(final RunningTask runningTask) {
    this.dispatch(RunningTask.class, runningTask);
  }

  public void onTaskCompleted(final CompletedTask completedTask) {
    this.dispatch(CompletedTask.class, completedTask);
  }

  public void onTaskSuspended(final SuspendedTask suspendedTask) {
    this.dispatch(SuspendedTask.class, suspendedTask);
  }

  public void onTaskMessage(final TaskMessage taskMessage) {
    this.dispatch(TaskMessage.class, taskMessage);
  }

  public void onTaskFailed(final FailedTask failedTask) {
    this.dispatch(FailedTask.class, failedTask);
  }

  public void onContextActive(final ActiveContext activeContext) {
    this.dispatch(ActiveContext.class, activeContext);
  }

  public void onContextClose(final ClosedContext closedContext) {
    this.dispatch(ClosedContext.class, closedContext);
  }

  public void onContextFailed(final FailedContext failedContext) {
    this.dispatch(FailedContext.class, failedContext);
  }

  public void onContextMessage(final ContextMessage contextMessage) {
    this.dispatch(ContextMessage.class, contextMessage);
  }

  public void onDriverRestartTaskRunning(final RunningTask runningTask) {
    this.dispatchForRestartedDriver(RunningTask.class, runningTask);
  }

  public void OnDriverRestartContextActive(final ActiveContext activeContext){
    this.dispatchForRestartedDriver(ActiveContext.class, activeContext);
  }

  public void OnDriverRestartCompleted(final DriverRestartCompleted restartCompleted){
    this.dispatchForRestartedDriver(DriverRestartCompleted.class, restartCompleted);
  }

  boolean isEmpty() {
    return this.applicationDispatcher.isEmpty();
  }

  private <T, U extends T> void dispatch(final Class<T> type, final U message) {
    this.serviceDispatcher.onNext(type, message);
    this.applicationDispatcher.onNext(type, message);
  }

  private <T, U extends T> void dispatchForRestartedDriver(final Class<T> type, final U message) {
    this.driverRestartApplicationDispatcher.onNext(type, message);
    this.driverRestartServiceDispatcher.onNext(type, message);
  }
}
