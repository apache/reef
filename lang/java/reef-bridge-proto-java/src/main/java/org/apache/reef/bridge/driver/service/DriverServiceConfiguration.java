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
package org.apache.reef.bridge.driver.service;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.service.parameters.DriverClientCommand;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.driver.parameters.DriverIdleSources;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.runtime.common.driver.client.JobStatusHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.HttpServerReefEventHandler;
import org.apache.reef.webserver.ReefEventStateManager;

/**
 * Binds all driver bridge service handlers to the driver.
 */
@Private
public final class DriverServiceConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredImpl<IDriverService> DRIVER_SERVICE_IMPL = new RequiredImpl<>();

  public static final RequiredParameter<String> DRIVER_CLIENT_COMMAND = new RequiredParameter<>();

  /** Configuration module that binds all driver handlers. */
  public static final ConfigurationModule CONF = new DriverServiceConfiguration()
      .bindImplementation(IDriverService.class, DRIVER_SERVICE_IMPL)
      .bindNamedParameter(DriverClientCommand.class, DRIVER_CLIENT_COMMAND)
      .bindSetEntry(DriverIdleSources.class, IDriverService.class)
      .build();


  public static final ConfigurationModule BASE_DRIVER_CONF = DriverConfiguration.CONF
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
      .set(DriverConfiguration.ON_CLIENT_MESSAGE, DriverServiceHandlers.ClientMessageHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED, DriverServiceHandlers.ClientCloseHandler.class)
      .set(DriverConfiguration.ON_CLIENT_CLOSED_MESSAGE, DriverServiceHandlers.ClientCloseWithMessageHandler.class);

  public static final ConfigurationModule RESTART_DRIVER_CONF_MODULE = DriverRestartConfiguration.CONF
      .set(DriverRestartConfiguration.ON_DRIVER_RESTARTED,
          DriverServiceHandlers.DriverRestartHandler.class)
      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_COMPLETED,
          DriverServiceHandlers.DriverRestartCompletedHandler.class)
      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_TASK_RUNNING,
          DriverServiceHandlers.DriverRestartRunningTaskHandler.class)
      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_EVALUATOR_FAILED,
          DriverServiceHandlers.DriverRestartFailedEvaluatorHandler.class)
      .set(DriverRestartConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE,
          DriverServiceHandlers.DriverRestartActiveContextHandler.class);


  /**
   * The HTTP Server configuration assumed by the bridge.
   */
  private static final Configuration HTTP_SERVER_CONFIGURATION = Configurations.merge(
      HttpHandlerConfiguration.CONF
          .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
          // Add the http status handler.
          .set(HttpHandlerConfiguration.HTTP_HANDLERS, DriverStatusHTTPHandler.class)
          .build(),
      org.apache.reef.client.DriverServiceConfiguration.CONF
          .set(org.apache.reef.client.DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED,
              ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
          .set(org.apache.reef.client.DriverServiceConfiguration.ON_CONTEXT_ACTIVE,
              ReefEventStateManager.ActiveContextStateHandler.class)
          .set(org.apache.reef.client.DriverServiceConfiguration.ON_TASK_RUNNING,
              ReefEventStateManager.TaskRunningStateHandler.class)
          .set(org.apache.reef.client.DriverServiceConfiguration.ON_DRIVER_STARTED,
              ReefEventStateManager.StartStateHandler.class)
          .set(org.apache.reef.client.DriverServiceConfiguration.ON_DRIVER_STOP,
              ReefEventStateManager.StopStateHandler.class)
          .build(),
      DriverRestartConfiguration.CONF
          .set(DriverRestartConfiguration.ON_DRIVER_RESTARTED,
              ReefEventStateManager.DriverRestartHandler.class)
          .set(DriverRestartConfiguration.ON_DRIVER_RESTART_CONTEXT_ACTIVE,
              ReefEventStateManager.DriverRestartActiveContextStateHandler.class)
          .set(DriverRestartConfiguration.ON_DRIVER_RESTART_TASK_RUNNING,
              ReefEventStateManager.DriverRestartTaskRunningStateHandler.class)
          .build(),
      // Bind the HTTP handler for job status
      Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(JobStatusHandler.class, DriverStatusHTTPHandler.class)
          .build()
  );

  /**
   * The name server configuration assumed by the bridge.
   */
  private static final Configuration NAME_SERVER_CONFIGURATION = NameServerConfiguration.CONF
      .set(NameServerConfiguration.NAME_SERVICE_PORT, 0)
      .build();

  /**
   * The driver configuration assumed by the the bridge.
   */
  public static final Configuration HTTP_AND_NAMESERVER = Configurations.merge(
      HTTP_SERVER_CONFIGURATION,
      NAME_SERVER_CONFIGURATION
  );
}
