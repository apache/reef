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
package org.apache.reef.client.parameters;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.parameters.DriverRestartCompletedHandlers;
import org.apache.reef.driver.parameters.DriverRestartContextActiveHandlers;
import org.apache.reef.driver.parameters.DriverRestartHandler;
import org.apache.reef.driver.parameters.DriverRestartTaskRunningHandlers;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.runtime.common.DriverRestartCompleted;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalImpl;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

public final class DriverRestartHandlersConfiguration extends ConfigurationModuleBuilder {
  /**
   * This event is fired in place of the ON_DRIVER_STARTED when the Driver is in fact restarted after failure.
   */
  public static final OptionalImpl<EventHandler<StartTime>> ON_DRIVER_RESTARTED = new OptionalImpl<>();

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

  public static final ConfigurationModule CONF = new DriverRestartHandlersConfiguration()
      .bindSetEntry(DriverRestartHandler.class, ON_DRIVER_RESTARTED)
      .bindSetEntry(DriverRestartTaskRunningHandlers.class, ON_DRIVER_RESTART_TASK_RUNNING)
      .bindSetEntry(DriverRestartContextActiveHandlers.class, ON_DRIVER_RESTART_CONTEXT_ACTIVE)
      .bindSetEntry(DriverRestartCompletedHandlers.class, ON_DRIVER_RESTART_COMPLETED)
      .build();
}
