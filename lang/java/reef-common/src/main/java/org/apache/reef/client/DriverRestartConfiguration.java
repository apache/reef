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
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.wake.EventHandler;

/**
 * EventHandlers specific to Driver Restart. Please remember to bind a runtime-specific DriverRestartConfiguration,
 * e.g. YarnDriverRestartConfiguration.
 */
@Public
@ClientSide
@Provided
@Unstable
public final class DriverRestartConfiguration extends ConfigurationModuleBuilder {
  /**
   * This event is fired in place of the ON_DRIVER_STARTED when the Driver is in fact restarted after failure.
   */
  public static final OptionalImpl<EventHandler<DriverRestarted>> ON_DRIVER_RESTARTED = new OptionalImpl<>();

  /**
   * Event handler for running tasks in previous evaluator, when driver restarted. Defaults to crash if not bound.
   */
  public static final OptionalImpl<EventHandler<RunningTask>> ON_DRIVER_RESTART_TASK_RUNNING = new OptionalImpl<>();

  /**
   * Event handler for active context when driver restart. Defaults to closing the context if not bound.
   */
  public static final OptionalImpl<EventHandler<ActiveContext>> ON_DRIVER_RESTART_CONTEXT_ACTIVE = new OptionalImpl<>();

  /**
   * Event handler for the event of driver restart completion, default to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<DriverRestartCompleted>> ON_DRIVER_RESTART_COMPLETED =
      new OptionalImpl<>();

  /**
   * Event handler for the event of driver restart completion, default to logging if not bound.
   */
  public static final OptionalImpl<EventHandler<FailedEvaluator>> ON_DRIVER_RESTART_EVALUATOR_FAILED =
      new OptionalImpl<>();

  /**
   * The amount of time in seconds the driver waits for evaluators to report back on restart.
   * Defaults to 3 minutes. If the value is set to Integer.MAX_VALUE, the driver will wait forever.
   */
  public static final OptionalParameter<Integer> DRIVER_RESTART_EVALUATOR_RECOVERY_SECONDS = new OptionalParameter<>();

  /**
   * Parameter to determine whether the driver should fail or continue if there are evaluator
   * preservation log failures. Defaults to false.
   */
  public static final OptionalParameter<Boolean> FAIL_DRIVER_ON_EVALUATOR_LOG_ERROR =
      new OptionalParameter<>();

  public static final ConfigurationModule CONF = new DriverRestartConfiguration()
      .bindNamedParameter(FailDriverOnEvaluatorLogErrors.class, FAIL_DRIVER_ON_EVALUATOR_LOG_ERROR)
      .bindNamedParameter(DriverRestartEvaluatorRecoverySeconds.class, DRIVER_RESTART_EVALUATOR_RECOVERY_SECONDS)
      .bindSetEntry(DriverRestartHandler.class, ON_DRIVER_RESTARTED)
      .bindSetEntry(DriverRestartTaskRunningHandlers.class, ON_DRIVER_RESTART_TASK_RUNNING)
      .bindSetEntry(DriverRestartContextActiveHandlers.class, ON_DRIVER_RESTART_CONTEXT_ACTIVE)
      .bindSetEntry(DriverRestartCompletedHandlers.class, ON_DRIVER_RESTART_COMPLETED)
      .bindSetEntry(DriverRestartFailedEvaluatorHandlers.class, ON_DRIVER_RESTART_EVALUATOR_FAILED)
      .build();

  private DriverRestartConfiguration(){
  }
}
