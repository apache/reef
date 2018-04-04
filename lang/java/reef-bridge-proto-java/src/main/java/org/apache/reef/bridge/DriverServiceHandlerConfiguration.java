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
package org.apache.reef.bridge;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;

/**
 * Binds all driver bridge service handlers to the driver.
 */
@Private
public final class DriverServiceHandlerConfiguration extends ConfigurationModuleBuilder {

  /** Configuration module that binds all driver handlers. */
  private static final ConfigurationModule CONF = new DriverServiceHandlerConfiguration()
      .merge(DriverConfiguration.CONF)
      .build()
      .set(DriverConfiguration.ON_DRIVER_STARTED, DriverServiceHandlers.StartHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STOP, DriverServiceHandlers.StopHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverServiceHandlers.AllocatedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, DriverServiceHandlers.CompletedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_FAILED, DriverServiceHandlers.FailedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DriverServiceHandlers.ActiveContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_CLOSED, DriverServiceHandlers.ClosedContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_FAILED, DriverServiceHandlers.ContextFailedHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DriverServiceHandlers.ContextMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_RUNNING, DriverServiceHandlers.RunningTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, DriverServiceHandlers.CompletedTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_FAILED, DriverServiceHandlers.FailedTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_MESSAGE, DriverServiceHandlers.TaskMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_SUSPENDED, DriverServiceHandlers.SuspendedTaskHandler.class)
      .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverServiceHandlers.ClientMessageHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverServiceHandlers.ClientCloseHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE,
          DriverServiceHandlers.ClientCloseWithMessageHandler.class);
}
