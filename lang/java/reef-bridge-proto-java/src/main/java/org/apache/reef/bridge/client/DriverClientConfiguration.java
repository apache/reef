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

import org.apache.reef.bridge.client.parameters.DriverClientDispatchThreadCount;
import org.apache.reef.bridge.client.parameters.ClientDriverStopHandler;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

/**
 * Driver client configuration.
 */
public final class DriverClientConfiguration extends ConfigurationModuleBuilder {

  /**
   * The event handler invoked right after the driver boots up.
   */
  public static final RequiredImpl<EventHandler<StartTime>> ON_DRIVER_STARTED = new RequiredImpl<>();

  /**
   * The event handler invoked right before the driver shuts down. Defaults to ignore.
   */
  public static final OptionalImpl<EventHandler<StopTime>> ON_DRIVER_STOP = new OptionalImpl<>();

  // ***** EVALUATOR HANDLER BINDINGS:

  /**
   * Event handler for allocated evaluators. Defaults to returning the evaluator if not bound.
   */
  public static final OptionalImpl<EventHandler<AllocatedEvaluator>> ON_EVALUATOR_ALLOCATED = new OptionalImpl<>();

  /**
   * Event handler for completed evaluators. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<CompletedEvaluator>> ON_EVALUATOR_COMPLETED = new OptionalImpl<>();

  /**
   * Event handler for failed evaluators. Defaults to job failure if not bound.
   */
  public static final OptionalImpl<EventHandler<FailedEvaluator>> ON_EVALUATOR_FAILED = new OptionalImpl<>();

  // ***** TASK HANDLER BINDINGS:

  /**
   * Event handler for task messages. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<TaskMessage>> ON_TASK_MESSAGE = new OptionalImpl<>();

  /**
   * Event handler for completed tasks. Defaults to closing the context the task ran on if not bound.
   */
  public static final OptionalImpl<EventHandler<CompletedTask>> ON_TASK_COMPLETED = new OptionalImpl<>();

  /**
   * Event handler for failed tasks. Defaults to job failure if not bound.
   */
  public static final OptionalImpl<EventHandler<FailedTask>> ON_TASK_FAILED = new OptionalImpl<>();

  /**
   * Event handler for running tasks. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<RunningTask>> ON_TASK_RUNNING = new OptionalImpl<>();

  /**
   * Event handler for suspended tasks. Defaults to job failure if not bound. Rationale: many jobs don't support
   * task suspension. Hence, this parameter should be optional. The only sane default is to crash the job, then.
   */
  public static final OptionalImpl<EventHandler<SuspendedTask>> ON_TASK_SUSPENDED = new OptionalImpl<>();

  // ***** CLIENT HANDLER BINDINGS:

  /**
   * Event handler for client messages. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<byte[]>> ON_CLIENT_MESSAGE = new OptionalImpl<>();

  /**
   * Event handler for close messages sent by the client. Defaults to job failure if not bound.
   */
  public static final OptionalImpl<EventHandler<Void>> ON_CLIENT_CLOSED = new OptionalImpl<>();

  /**
   * Event handler for close messages sent by the client. Defaults to job failure if not bound.
   */
  public static final OptionalImpl<EventHandler<byte[]>> ON_CLIENT_CLOSED_MESSAGE = new OptionalImpl<>();

  // ***** CONTEXT HANDLER BINDINGS:

  /**
   * Event handler for active context. Defaults to closing the context if not bound.
   */
  public static final OptionalImpl<EventHandler<ActiveContext>> ON_CONTEXT_ACTIVE = new OptionalImpl<>();

  /**
   * Event handler for closed context. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ClosedContext>> ON_CONTEXT_CLOSED = new OptionalImpl<>();

  /**
   * Event handler for closed context. Defaults to job failure if not bound.
   */
  public static final OptionalImpl<EventHandler<FailedContext>> ON_CONTEXT_FAILED = new OptionalImpl<>();

  /**
   * Event handler for context messages. Defaults to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<ContextMessage>> ON_CONTEXT_MESSAGE = new OptionalImpl<>();

  /**
   * Number of dispatch threads to use.
   */
  public static final OptionalImpl<Integer> CLIENT_DRIVER_DISPATCH_THREAD_COUNT = new OptionalImpl<>();

  /**
   * Alarm dispatch handler.
   */
  public static final OptionalImpl<IAlarmDispatchHandler> ALARM_DISPATCH_HANDLER = new OptionalImpl<>();

  /**
   * Default to gRPC Driver Client Service.
   */
  public static final OptionalImpl<IDriverClientService> DRIVER_CLIENT_SERVICE = new OptionalImpl<>();

  /**
   * Default to gRPC Driver Service Client.
   */
  public static final OptionalImpl<IDriverServiceClient> DRIVER_SERVICE_CLIENT = new OptionalImpl<>();

  /**
   * ConfigurationModule to fill out to get a legal Driver Configuration.
   */
  public static final ConfigurationModule CONF = new DriverClientConfiguration()
      .bindImplementation(Clock.class, DriverClientClock.class)
      .bindImplementation(EvaluatorRequestor.class, DriverClientEvaluatorRequestor.class)
      .bindImplementation(IAlarmDispatchHandler.class, ALARM_DISPATCH_HANDLER)
      .bindImplementation(IDriverClientService.class, DRIVER_CLIENT_SERVICE)
      .bindImplementation(IDriverServiceClient.class, DRIVER_SERVICE_CLIENT)

      .bindNamedParameter(DriverClientDispatchThreadCount.class, CLIENT_DRIVER_DISPATCH_THREAD_COUNT)

      // Driver start/stop handlers
      .bindSetEntry(DriverStartHandler.class, ON_DRIVER_STARTED)
      .bindSetEntry(ClientDriverStopHandler.class, ON_DRIVER_STOP)

      // Evaluator handlers
      .bindSetEntry(EvaluatorAllocatedHandlers.class, ON_EVALUATOR_ALLOCATED)
      .bindSetEntry(EvaluatorCompletedHandlers.class, ON_EVALUATOR_COMPLETED)
      .bindSetEntry(EvaluatorFailedHandlers.class, ON_EVALUATOR_FAILED)

      // Task handlers
      .bindSetEntry(TaskRunningHandlers.class, ON_TASK_RUNNING)
      .bindSetEntry(TaskFailedHandlers.class, ON_TASK_FAILED)
      .bindSetEntry(TaskMessageHandlers.class, ON_TASK_MESSAGE)
      .bindSetEntry(TaskCompletedHandlers.class, ON_TASK_COMPLETED)
      .bindSetEntry(TaskSuspendedHandlers.class, ON_TASK_SUSPENDED)

      // Context handlers
      .bindSetEntry(ContextActiveHandlers.class, ON_CONTEXT_ACTIVE)
      .bindSetEntry(ContextClosedHandlers.class, ON_CONTEXT_CLOSED)
      .bindSetEntry(ContextMessageHandlers.class, ON_CONTEXT_MESSAGE)
      .bindSetEntry(ContextFailedHandlers.class, ON_CONTEXT_FAILED)

      // Client handlers
      .bindSetEntry(ClientMessageHandlers.class, ON_CLIENT_MESSAGE)
      .bindSetEntry(ClientCloseHandlers.class, ON_CLIENT_CLOSED)
      .bindSetEntry(ClientCloseWithMessageHandlers.class, ON_CLIENT_CLOSED_MESSAGE)

      .build();
}
