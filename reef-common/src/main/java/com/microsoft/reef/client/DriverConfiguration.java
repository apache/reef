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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
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
import com.microsoft.reef.runtime.common.driver.DriverRuntimeConfiguration;
import com.microsoft.tang.formats.*;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

/**
 * A ConfigurationModule for Drivers.
 */
@ClientSide
@Public
@Provided
public final class DriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * Identifies the driver and therefore the JOB. Expect to see this e.g. on YARN's dashboard.
   */
  public static final RequiredParameter<String> DRIVER_IDENTIFIER = new RequiredParameter<>();

  /**
   * The amount of memory to be allocated for the Driver. This is the size of the AM container in YARN.
   */
  public static final OptionalParameter<Integer> DRIVER_MEMORY = new OptionalParameter<>();

  /**
   * Files to be made available on the Driver and all Evaluators.
   */
  public static final OptionalParameter<String> GLOBAL_FILES = new OptionalParameter<>();

  /**
   * Libraries to be made available on the Driver and all Evaluators.
   */
  public static final OptionalParameter<String> GLOBAL_LIBRARIES = new OptionalParameter<>();

  /**
   * Files to be made available on the Driver only.
   */
  public static final OptionalParameter<String> LOCAL_FILES = new OptionalParameter<>();

  /**
   * Libraries to be made available on the Driver only.
   */
  public static final OptionalParameter<String> LOCAL_LIBRARIES = new OptionalParameter<>();

  /**
   * Job submission directory to be used by driver. This is the folder on the DFS used to stage the files
   * for the Driver and subsequently for the Evaluators. It will be created if it doesn't exist yet.
   * If this is set by the user, user must make sure its uniqueness across different jobs.
   */
  public static final OptionalParameter<String> DRIVER_JOB_SUBMISSION_DIRECTORY = new OptionalParameter<>();

  /**
   * The event handler invoked right after the driver boots up.
   */
  public static final RequiredImpl<EventHandler<StartTime>> ON_DRIVER_STARTED = new RequiredImpl<>();

  /**
   * This event is fired in place of the ON_DRIVER_STARTED when the Driver is in fact restarted after failure.
   */
  public static final OptionalImpl<EventHandler<StartTime>> ON_DRIVER_RESTARTED = new OptionalImpl<>();

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
   * Event handler for running tasks in previous evaluator, when driver restarted. Defaults to crash if not bound.
   */
  public static final OptionalImpl<EventHandler<RunningTask>> ON_DRIVER_RESTART_TASK_RUNNING = new OptionalImpl<>();

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
   * Event handler for active context when driver restart. Defaults to closing the context if not bound.
   */
  public static final OptionalImpl<EventHandler<ActiveContext>> ON_DRIVER_RESTART_CONTEXT_ACTIVE = new OptionalImpl<>();

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
   * "Number of threads allocated per evaluator to dispatch events from this Evaluator.
   */
  public static final OptionalParameter<Integer> EVALUATOR_DISPATCHER_THREADS = new OptionalParameter<>();

  /**
   * Event handler for the event of driver restart completion, default to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<DriverRestartCompleted>> ON_DRIVER_RESTART_COMPLETED = new OptionalImpl<>();

  /**
   * ConfigurationModule to fill out to get a legal Driver Configuration.
   */
  public static final ConfigurationModule CONF = new DriverConfiguration().merge(DriverRuntimeConfiguration.CONF)

      .bindNamedParameter(DriverIdentifier.class, DRIVER_IDENTIFIER)
      .bindNamedParameter(DriverMemory.class, DRIVER_MEMORY)
      .bindNamedParameter(DriverJobSubmissionDirectory.class, DRIVER_JOB_SUBMISSION_DIRECTORY)
      .bindSetEntry(JobGlobalFiles.class, GLOBAL_FILES)
      .bindSetEntry(JobGlobalLibraries.class, GLOBAL_LIBRARIES)
      .bindSetEntry(DriverLocalFiles.class, LOCAL_FILES)
      .bindSetEntry(DriverLocalLibraries.class, LOCAL_LIBRARIES)

          // Driver start/stop handlers
      .bindSetEntry(DriverStartHandler.class, ON_DRIVER_STARTED)
      .bindNamedParameter(DriverRestartHandler.class, ON_DRIVER_RESTARTED)
      .bindSetEntry(Clock.StartHandler.class, com.microsoft.reef.runtime.common.driver.DriverStartHandler.class)
      .bindSetEntry(Clock.StopHandler.class, ON_DRIVER_STOP)

          // Evaluator handlers
      .bindSetEntry(EvaluatorAllocatedHandlers.class, ON_EVALUATOR_ALLOCATED)
      .bindSetEntry(EvaluatorCompletedHandlers.class, ON_EVALUATOR_COMPLETED)
      .bindSetEntry(EvaluatorFailedHandlers.class, ON_EVALUATOR_FAILED)

          // Task handlers
      .bindSetEntry(TaskRunningHandlers.class, ON_TASK_RUNNING)
      .bindSetEntry(DriverRestartTaskRunningHandlers.class, ON_DRIVER_RESTART_TASK_RUNNING)
      .bindSetEntry(TaskFailedHandlers.class, ON_TASK_FAILED)
      .bindSetEntry(TaskMessageHandlers.class, ON_TASK_MESSAGE)
      .bindSetEntry(TaskCompletedHandlers.class, ON_TASK_COMPLETED)
      .bindSetEntry(TaskSuspendedHandlers.class, ON_TASK_SUSPENDED)

          // Context handlers
      .bindSetEntry(ContextActiveHandlers.class, ON_CONTEXT_ACTIVE)
      .bindSetEntry(DriverRestartContextActiveHandlers.class, ON_DRIVER_RESTART_CONTEXT_ACTIVE)
      .bindSetEntry(ContextClosedHandlers.class, ON_CONTEXT_CLOSED)
      .bindSetEntry(ContextMessageHandlers.class, ON_CONTEXT_MESSAGE)
      .bindSetEntry(ContextFailedHandlers.class, ON_CONTEXT_FAILED)

          // Client handlers
      .bindSetEntry(ClientMessageHandlers.class, ON_CLIENT_MESSAGE)
      .bindSetEntry(ClientCloseHandlers.class, ON_CLIENT_CLOSED)
      .bindSetEntry(ClientCloseWithMessageHandlers.class, ON_CLIENT_CLOSED_MESSAGE)

          // Various parameters
      .bindNamedParameter(EvaluatorDispatcherThreads.class, EVALUATOR_DISPATCHER_THREADS)
      .bindSetEntry(DriverRestartCompletedHandlers.class, ON_DRIVER_RESTART_COMPLETED)
      .build();
}
