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

package org.apache.reef.bridge.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.client.parameters.DriverClientDispatchThreadCount;
import org.apache.reef.bridge.client.parameters.ClientDriverStopHandler;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.runtime.common.utils.DispatchingEStage;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.Set;

/**
 * Async dispatch of client driver events.
 */
@Private
public final class DriverClientDispatcher {

  /**
   * Dispatcher used for application provided event handlers.
   */
  private final DispatchingEStage applicationDispatcher;

  /**
   * Dispatcher for client close events.
   */
  private final DispatchingEStage clientCloseDispatcher;

  /**
   * Dispatcher for client close with message events.
   */
  private final DispatchingEStage clientCloseWithMessageDispatcher;

  /**
   * Dispatcher for client messages.
   */
  private final DispatchingEStage clientMessageDispatcher;

  @Inject
  private DriverClientDispatcher(
      final DriverClientExceptionHandler driverExceptionHandler,
      @Parameter(DriverClientDispatchThreadCount.class)
      final Integer numberOfThreads,
      // Application-provided start and stop handlers
      @Parameter(DriverStartHandler.class)
      final Set<EventHandler<StartTime>> startHandlers,
      @Parameter(ClientDriverStopHandler.class)
      final Set<EventHandler<StopTime>> stopHandlers,
      // Application-provided Context event handlers
      @Parameter(ContextActiveHandlers.class)
      final Set<EventHandler<ActiveContext>> contextActiveHandlers,
      @Parameter(ContextClosedHandlers.class)
      final Set<EventHandler<ClosedContext>> contextClosedHandlers,
      @Parameter(ContextFailedHandlers.class)
      final Set<EventHandler<FailedContext>> contextFailedHandlers,
      @Parameter(ContextMessageHandlers.class)
      final Set<EventHandler<ContextMessage>> contextMessageHandlers,
      // Application-provided Task event handlers
      @Parameter(TaskRunningHandlers.class)
      final Set<EventHandler<RunningTask>> taskRunningHandlers,
      @Parameter(TaskCompletedHandlers.class)
      final Set<EventHandler<CompletedTask>> taskCompletedHandlers,
      @Parameter(TaskSuspendedHandlers.class)
      final Set<EventHandler<SuspendedTask>> taskSuspendedHandlers,
      @Parameter(TaskMessageHandlers.class)
      final Set<EventHandler<TaskMessage>> taskMessageEventHandlers,
      @Parameter(TaskFailedHandlers.class)
      final Set<EventHandler<FailedTask>> taskExceptionEventHandlers,
      // Application-provided Evaluator event handlers
      @Parameter(EvaluatorAllocatedHandlers.class)
      final Set<EventHandler<AllocatedEvaluator>> evaluatorAllocatedHandlers,
      @Parameter(EvaluatorFailedHandlers.class)
      final Set<EventHandler<FailedEvaluator>> evaluatorFailedHandlers,
      @Parameter(EvaluatorCompletedHandlers.class)
      final Set<EventHandler<CompletedEvaluator>> evaluatorCompletedHandlers,
      // Client handlers
      @Parameter(ClientCloseHandlers.class)
      final Set<EventHandler<Void>> clientCloseHandlers,
      @Parameter(ClientCloseWithMessageHandlers.class)
      final Set<EventHandler<byte[]>> clientCloseWithMessageHandlers,
      @Parameter(ClientMessageHandlers.class)
      final Set<EventHandler<byte[]>> clientMessageHandlers) {

    this.applicationDispatcher = new DispatchingEStage(
        driverExceptionHandler, numberOfThreads, "ClientDriverDispatcher");
    // Application start and stop handlers
    this.applicationDispatcher.register(StartTime.class, startHandlers);
    this.applicationDispatcher.register(StopTime.class, stopHandlers);
    // Application Context event handlers
    this.applicationDispatcher.register(ActiveContext.class, contextActiveHandlers);
    this.applicationDispatcher.register(ClosedContext.class, contextClosedHandlers);
    this.applicationDispatcher.register(FailedContext.class, contextFailedHandlers);
    this.applicationDispatcher.register(ContextMessage.class, contextMessageHandlers);

    // Application Task event handlers.
    this.applicationDispatcher.register(RunningTask.class, taskRunningHandlers);
    this.applicationDispatcher.register(CompletedTask.class, taskCompletedHandlers);
    this.applicationDispatcher.register(SuspendedTask.class, taskSuspendedHandlers);
    this.applicationDispatcher.register(TaskMessage.class, taskMessageEventHandlers);
    this.applicationDispatcher.register(FailedTask.class, taskExceptionEventHandlers);

    // Application Evaluator event handlers
    this.applicationDispatcher.register(AllocatedEvaluator.class, evaluatorAllocatedHandlers);
    this.applicationDispatcher.register(CompletedEvaluator.class, evaluatorCompletedHandlers);
    this.applicationDispatcher.register(FailedEvaluator.class, evaluatorFailedHandlers);

    // Client event handlers;
    this.clientCloseDispatcher = new DispatchingEStage(this.applicationDispatcher);
    this.clientCloseDispatcher.register(Void.class, clientCloseHandlers);

    this.clientCloseWithMessageDispatcher = new DispatchingEStage(this.applicationDispatcher);
    this.clientCloseWithMessageDispatcher.register(byte[].class, clientCloseWithMessageHandlers);

    this.clientMessageDispatcher = new DispatchingEStage(this.applicationDispatcher);
    this.clientMessageDispatcher.register(byte[].class, clientMessageHandlers);
  }

  public void dispatch(final StartTime startTime) {
    this.applicationDispatcher.onNext(StartTime.class, startTime);
  }

  public void dispatch(final StopTime stopTime) {
    this.applicationDispatcher.onNext(StopTime.class, stopTime);
  }

  public void dispatch(final ActiveContext context) {
    this.applicationDispatcher.onNext(ActiveContext.class, context);
  }

  public void dispatch(final ClosedContext context) {
    this.applicationDispatcher.onNext(ClosedContext.class, context);
  }

  public void dispatch(final FailedContext context) {
    this.applicationDispatcher.onNext(FailedContext.class, context);
  }

  public void dispatch(final ContextMessage message) {
    this.applicationDispatcher.onNext(ContextMessage.class, message);
  }

  public void dispatch(final AllocatedEvaluator evaluator) {
    this.applicationDispatcher.onNext(AllocatedEvaluator.class, evaluator);
  }

  public void dispatch(final FailedEvaluator evaluator) {
    this.applicationDispatcher.onNext(FailedEvaluator.class, evaluator);
  }

  public void dispatch(final CompletedEvaluator evaluator) {
    this.applicationDispatcher.onNext(CompletedEvaluator.class, evaluator);
  }

  public void dispatch(final RunningTask task) {
    this.applicationDispatcher.onNext(RunningTask.class, task);
  }

  public void dispatch(final CompletedTask task) {
    this.applicationDispatcher.onNext(CompletedTask.class, task);
  }

  public void dispatch(final FailedTask task) {
    this.applicationDispatcher.onNext(FailedTask.class, task);
  }

  public void dispatch(final SuspendedTask task) {
    this.applicationDispatcher.onNext(SuspendedTask.class, task);
  }

  public void dispatch(final TaskMessage message) {
    this.applicationDispatcher.onNext(TaskMessage.class, message);
  }

  public void clientCloseDispatch() {
    this.clientCloseDispatcher.onNext(Void.class, null);
  }

  public void clientCloseWithMessageDispatch(final byte[] message) {
    this.clientCloseWithMessageDispatcher.onNext(byte[].class, message);
  }

  public void clientMessageDispatch(final byte[] message) {
    this.clientMessageDispatcher.onNext(byte[].class, message);
  }
}
