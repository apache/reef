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
 * Binds all driver bridge serivce handlers to the driver.
 */
@Private
public final class DriverBridgeHandlerConfiguration extends ConfigurationModuleBuilder {

  /** Configuration module that binds all driver handlers. */
  private static final ConfigurationModule CONF = new DriverBridgeHandlerConfiguration()
      .merge(DriverConfiguration.CONF)
      .build()
      .set(DriverConfiguration.ON_DRIVER_STARTED, DriverBridgeServiceHandlers.StartHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STOP, DriverBridgeServiceHandlers.StopHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverBridgeServiceHandlers.AllocatedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, DriverBridgeServiceHandlers.CompletedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_FAILED, DriverBridgeServiceHandlers.FailedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DriverBridgeServiceHandlers.ActiveContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_CLOSED, DriverBridgeServiceHandlers.ClosedContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_FAILED, DriverBridgeServiceHandlers.ContextFailedHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_MESSAGE, DriverBridgeServiceHandlers.ContextMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_RUNNING, DriverBridgeServiceHandlers.RunningTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, DriverBridgeServiceHandlers.CompletedTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_FAILED, DriverBridgeServiceHandlers.FailedTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_MESSAGE, DriverBridgeServiceHandlers.TaskMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_SUSPENDED, DriverBridgeServiceHandlers.SuspendedTaskHandler.class)
      .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverBridgeServiceHandlers.ClientMessageHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverBridgeServiceHandlers.ClientCloseHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE,
          DriverBridgeServiceHandlers.ClientCloseWithMessageHandler.class);
}
