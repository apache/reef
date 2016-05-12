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
package org.apache.reef.client;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.formats.*;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

/**
 * Use this ConfigurationModule to configure Services to be run in the Driver.
 * <p>
 * A service is a set of event handlers that are informed of events in addition to * the event handlers defined in
 * DriverConfiguration. However, most services will treat the events as read-only. Doing differently should be
 * documented clearly in the Service documentation.
 */
@ClientSide
@Public
@Provided
public final class DriverServiceConfiguration extends ConfigurationModuleBuilder {


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
   * The event handler invoked right after the driver boots up.
   */
  public static final RequiredImpl<EventHandler<StartTime>> ON_DRIVER_STARTED = new RequiredImpl<>();

  /**
   * The event handler invoked right after the driver restarts.
   */
  public static final OptionalImpl<EventHandler<DriverRestarted>> ON_DRIVER_RESTARTED = new OptionalImpl<>();

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
   * ConfigurationModule to fill out to get a legal Driver Configuration.
   */
  public static final ConfigurationModule CONF = new DriverServiceConfiguration()
      // Files use the very same named parameters as the DriverConfiguration
      .bindSetEntry(JobGlobalFiles.class, GLOBAL_FILES)
      .bindSetEntry(JobGlobalLibraries.class, GLOBAL_LIBRARIES)
      .bindSetEntry(DriverLocalFiles.class, LOCAL_FILES)
      .bindSetEntry(DriverLocalLibraries.class, LOCAL_LIBRARIES)

          // Start and stop events are the same handlers for applications and services.
      .bindSetEntry(Clock.StartHandler.class, ON_DRIVER_STARTED)
      .bindSetEntry(Clock.StopHandler.class, ON_DRIVER_STOP)
      .bindSetEntry(ServiceDriverRestartedHandlers.class, ON_DRIVER_RESTARTED)

          // Evaluator handlers
      .bindSetEntry(ServiceEvaluatorAllocatedHandlers.class, ON_EVALUATOR_ALLOCATED)
      .bindSetEntry(ServiceEvaluatorCompletedHandlers.class, ON_EVALUATOR_COMPLETED)
      .bindSetEntry(ServiceEvaluatorFailedHandlers.class, ON_EVALUATOR_FAILED)

          // Task handlers
      .bindSetEntry(ServiceTaskRunningHandlers.class, ON_TASK_RUNNING)
      .bindSetEntry(ServiceTaskFailedHandlers.class, ON_TASK_FAILED)
      .bindSetEntry(ServiceTaskMessageHandlers.class, ON_TASK_MESSAGE)
      .bindSetEntry(ServiceTaskCompletedHandlers.class, ON_TASK_COMPLETED)
      .bindSetEntry(ServiceTaskSuspendedHandlers.class, ON_TASK_SUSPENDED)

          // Context handlers
      .bindSetEntry(ServiceContextActiveHandlers.class, ON_CONTEXT_ACTIVE)
      .bindSetEntry(ServiceContextClosedHandlers.class, ON_CONTEXT_CLOSED)
      .bindSetEntry(ServiceContextMessageHandlers.class, ON_CONTEXT_MESSAGE)
      .bindSetEntry(ServiceContextFailedHandlers.class, ON_CONTEXT_FAILED)

      .build();
}
