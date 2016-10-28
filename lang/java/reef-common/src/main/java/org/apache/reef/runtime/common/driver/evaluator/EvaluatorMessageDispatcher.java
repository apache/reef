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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.runtime.common.driver.DriverExceptionHandler;
import org.apache.reef.runtime.common.utils.DispatchingEStage;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central dispatcher for all Evaluator related events. This exists once per Evaluator.
 */
public final class EvaluatorMessageDispatcher implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(EvaluatorMessageDispatcher.class.getName());

  private final String evaluatorIdentifier;

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
  private EvaluatorMessageDispatcher(
      // Application-provided Context event handlers
      @Parameter(ContextActiveHandlers.class) final Set<EventHandler<ActiveContext>> contextActiveHandlers,
      @Parameter(ContextClosedHandlers.class) final Set<EventHandler<ClosedContext>> contextClosedHandlers,
      @Parameter(ContextFailedHandlers.class) final Set<EventHandler<FailedContext>> contextFailedHandlers,
      @Parameter(ContextMessageHandlers.class) final Set<EventHandler<ContextMessage>> contextMessageHandlers,
      // Service-provided Context event handlers
      @Parameter(ServiceContextActiveHandlers.class)
      final Set<EventHandler<ActiveContext>> serviceContextActiveHandlers,
      @Parameter(ServiceContextClosedHandlers.class)
      final Set<EventHandler<ClosedContext>> serviceContextClosedHandlers,
      @Parameter(ServiceContextFailedHandlers.class)
      final Set<EventHandler<FailedContext>> serviceContextFailedHandlers,
      @Parameter(ServiceContextMessageHandlers.class)
      final Set<EventHandler<ContextMessage>> serviceContextMessageHandlers,
      // Application-provided Task event handlers
      @Parameter(TaskRunningHandlers.class) final Set<EventHandler<RunningTask>> taskRunningHandlers,
      @Parameter(TaskCompletedHandlers.class) final Set<EventHandler<CompletedTask>> taskCompletedHandlers,
      @Parameter(TaskSuspendedHandlers.class) final Set<EventHandler<SuspendedTask>> taskSuspendedHandlers,
      @Parameter(TaskMessageHandlers.class) final Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      @Parameter(TaskFailedHandlers.class) final Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      // Service-provided Task event handlers
      @Parameter(ServiceTaskRunningHandlers.class) final Set<EventHandler<RunningTask>> serviceTaskRunningEventHandlers,
      @Parameter(ServiceTaskCompletedHandlers.class)
      final Set<EventHandler<CompletedTask>> serviceTaskCompletedEventHandlers,
      @Parameter(ServiceTaskSuspendedHandlers.class)
      final Set<EventHandler<SuspendedTask>> serviceTaskSuspendedEventHandlers,
      @Parameter(ServiceTaskMessageHandlers.class) final Set<EventHandler<TaskMessage>> serviceTaskMessageEventHandlers,
      @Parameter(ServiceTaskFailedHandlers.class) final Set<EventHandler<FailedTask>> serviceTaskExceptionEventHandlers,
      // Application-provided Evaluator event handlers
      @Parameter(EvaluatorAllocatedHandlers.class)
      final Set<EventHandler<AllocatedEvaluator>> evaluatorAllocatedHandlers,
      @Parameter(EvaluatorFailedHandlers.class) final Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers,
      @Parameter(EvaluatorCompletedHandlers.class)
      final Set<EventHandler<CompletedEvaluator>> evaluatorCompletedHandlers,
      // Service-provided Evaluator event handlers
      @Parameter(ServiceEvaluatorAllocatedHandlers.class)
      final Set<EventHandler<AllocatedEvaluator>> serviceEvaluatorAllocatedEventHandlers,
      @Parameter(ServiceEvaluatorFailedHandlers.class)
      final Set<EventHandler<FailedEvaluator>> serviceEvaluatorFailedHandlers,
      @Parameter(ServiceEvaluatorCompletedHandlers.class)
      final Set<EventHandler<CompletedEvaluator>> serviceEvaluatorCompletedHandlers,

      // Application event handlers specific to a Driver restart
      @Parameter(DriverRestartTaskRunningHandlers.class)
      final Set<EventHandler<RunningTask>> driverRestartTaskRunningHandlers,
      @Parameter(DriverRestartContextActiveHandlers.class)
      final Set<EventHandler<ActiveContext>> driverRestartActiveContextHandlers,
      @Parameter(DriverRestartFailedEvaluatorHandlers.class)
      final Set<EventHandler<FailedEvaluator>> driverRestartEvaluatorFailedHandlers,

      // Service-provided event handlers specific to a Driver restart
      @Parameter(ServiceDriverRestartTaskRunningHandlers.class)
      final Set<EventHandler<RunningTask>> serviceDriverRestartTaskRunningHandlers,
      @Parameter(ServiceDriverRestartContextActiveHandlers.class)
      final Set<EventHandler<ActiveContext>> serviceDriverRestartActiveContextHandlers,
      @Parameter(ServiceDriverRestartFailedEvaluatorHandlers.class)
      final Set<EventHandler<FailedEvaluator>> serviceDriverRestartFailedEvaluatorHandlers,

      @Parameter(EvaluatorDispatcherThreads.class) final int numberOfThreads,
      @Parameter(EvaluatorManager.EvaluatorIdentifier.class) final String evaluatorIdentifier,
      final DriverExceptionHandler driverExceptionHandler,
      final IdlenessCallbackEventHandlerFactory idlenessCallbackEventHandlerFactory) {

    LOG.log(Level.FINER, "Creating message dispatcher for {0}", evaluatorIdentifier);

    this.evaluatorIdentifier = evaluatorIdentifier;
    this.serviceDispatcher = new DispatchingEStage(
        driverExceptionHandler, numberOfThreads, "EvaluatorMessageDispatcher:" + evaluatorIdentifier);

    this.applicationDispatcher = new DispatchingEStage(this.serviceDispatcher);
    this.driverRestartApplicationDispatcher = new DispatchingEStage(this.serviceDispatcher);
    this.driverRestartServiceDispatcher = new DispatchingEStage(this.serviceDispatcher);

    // Application Context event handlers
    this.applicationDispatcher.register(ActiveContext.class, contextActiveHandlers);
    this.applicationDispatcher.register(ClosedContext.class, contextClosedHandlers);
    this.applicationDispatcher.register(FailedContext.class, contextFailedHandlers);
    this.applicationDispatcher.register(ContextMessage.class, contextMessageHandlers);

    // Service Context event handlers
    this.serviceDispatcher.register(ActiveContext.class, serviceContextActiveHandlers);
    this.serviceDispatcher.register(ClosedContext.class, serviceContextClosedHandlers);
    this.serviceDispatcher.register(FailedContext.class, serviceContextFailedHandlers);
    this.serviceDispatcher.register(ContextMessage.class, serviceContextMessageHandlers);

    // Application Task event handlers.
    this.applicationDispatcher.register(RunningTask.class, taskRunningHandlers);
    this.applicationDispatcher.register(CompletedTask.class, taskCompletedHandlers);
    this.applicationDispatcher.register(SuspendedTask.class, taskSuspendedHandlers);
    this.applicationDispatcher.register(TaskMessage.class, taskMessageEventHandlers);
    this.applicationDispatcher.register(FailedTask.class, taskExceptionEventHandlers);

    // Service Task event handlers
    this.serviceDispatcher.register(RunningTask.class, serviceTaskRunningEventHandlers);
    this.serviceDispatcher.register(CompletedTask.class, serviceTaskCompletedEventHandlers);
    this.serviceDispatcher.register(SuspendedTask.class, serviceTaskSuspendedEventHandlers);
    this.serviceDispatcher.register(TaskMessage.class, serviceTaskMessageEventHandlers);
    this.serviceDispatcher.register(FailedTask.class, serviceTaskExceptionEventHandlers);

    // Application Evaluator event handlers
    this.applicationDispatcher.register(AllocatedEvaluator.class, evaluatorAllocatedHandlers);

    // Service Evaluator event handlers
    this.serviceDispatcher.register(FailedEvaluator.class, serviceEvaluatorFailedHandlers);
    this.serviceDispatcher.register(CompletedEvaluator.class, serviceEvaluatorCompletedHandlers);
    this.serviceDispatcher.register(AllocatedEvaluator.class, serviceEvaluatorAllocatedEventHandlers);

    // Application event handlers specific to a Driver restart
    this.driverRestartApplicationDispatcher.register(RunningTask.class, driverRestartTaskRunningHandlers);
    this.driverRestartApplicationDispatcher.register(ActiveContext.class, driverRestartActiveContextHandlers);

    final Set<EventHandler<FailedEvaluator>> driverRestartEvaluatorFailedCallbackHandlers = new HashSet<>();
    for (final EventHandler<FailedEvaluator> evaluatorFailedHandler : driverRestartEvaluatorFailedHandlers) {
      driverRestartEvaluatorFailedCallbackHandlers.add(
          idlenessCallbackEventHandlerFactory.createIdlenessCallbackWrapperHandler(evaluatorFailedHandler));
    }

    this.driverRestartApplicationDispatcher.register(
        FailedEvaluator.class, driverRestartEvaluatorFailedCallbackHandlers);

    // Service event handlers specific to a Driver restart
    this.driverRestartServiceDispatcher.register(RunningTask.class, serviceDriverRestartTaskRunningHandlers);
    this.driverRestartServiceDispatcher.register(ActiveContext.class, serviceDriverRestartActiveContextHandlers);
    this.driverRestartServiceDispatcher.register(FailedEvaluator.class, serviceDriverRestartFailedEvaluatorHandlers);

    final Set<EventHandler<CompletedEvaluator>> evaluatorCompletedCallbackHandlers = new HashSet<>();
    for (final EventHandler<CompletedEvaluator> evaluatorCompletedHandler : evaluatorCompletedHandlers) {
      evaluatorCompletedCallbackHandlers.add(
          idlenessCallbackEventHandlerFactory.createIdlenessCallbackWrapperHandler(evaluatorCompletedHandler));
    }
    this.applicationDispatcher.register(CompletedEvaluator.class, evaluatorCompletedCallbackHandlers);

    final Set<EventHandler<FailedEvaluator>> evaluatorFailedCallbackHandlers = new HashSet<>();
    for (final EventHandler<FailedEvaluator> evaluatorFailedHandler : evaluatorFailedHandlers) {
      evaluatorFailedCallbackHandlers.add(
          idlenessCallbackEventHandlerFactory.createIdlenessCallbackWrapperHandler(evaluatorFailedHandler));
    }
    this.applicationDispatcher.register(FailedEvaluator.class, evaluatorFailedCallbackHandlers);

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

  public void onDriverRestartContextActive(final ActiveContext activeContext) {
    this.dispatchForRestartedDriver(ActiveContext.class, activeContext);
  }

  public void onDriverRestartEvaluatorFailed(final FailedEvaluator failedEvaluator) {
    this.dispatchForRestartedDriver(FailedEvaluator.class, failedEvaluator);
  }

  boolean isEmpty() {
    return this.applicationDispatcher.isEmpty();
  }

  private <T, U extends T> void dispatch(final Class<T> type, final U message) {
    this.serviceDispatcher.onNext(type, message);
    this.applicationDispatcher.onNext(type, message);
  }

  private <T, U extends T> void dispatchForRestartedDriver(final Class<T> type, final U message) {
    this.driverRestartServiceDispatcher.onNext(type, message);
    this.driverRestartApplicationDispatcher.onNext(type, message);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINER, "Closing message dispatcher for {0}", this.evaluatorIdentifier);
    // This effectively closes all dispatchers as they share the same stage.
    this.serviceDispatcher.close();
  }
}
