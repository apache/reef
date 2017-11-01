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

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverServiceConfiguration;
import org.apache.reef.client.DriverRestartConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.javabridge.generic.JobDriver;
import org.apache.reef.runtime.common.driver.client.JobStatusHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.HttpServerReefEventHandler;
import org.apache.reef.webserver.ReefEventStateManager;

/**
 * Constant Configuration instances used by the bridge.
 */
public final class Constants {

  private static final Tang TANG = Tang.Factory.getTang();

  /**
   * Contains all bindings of event handlers to the bridge.
   */
  public static final Configuration DRIVER_CONFIGURATION = DriverConfiguration.CONF
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, JobDriver.AllocatedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_FAILED, JobDriver.FailedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_ACTIVE, JobDriver.ActiveContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_CLOSED, JobDriver.ClosedContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_FAILED, JobDriver.FailedContextHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_MESSAGE, JobDriver.ContextMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_MESSAGE, JobDriver.TaskMessageHandler.class)
      .set(DriverConfiguration.ON_TASK_FAILED, JobDriver.FailedTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_RUNNING, JobDriver.RunningTaskHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, JobDriver.CompletedTaskHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STARTED, JobDriver.StartHandler.class)
      .set(DriverConfiguration.ON_TASK_SUSPENDED, JobDriver.SuspendedTaskHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, JobDriver.CompletedEvaluatorHandler.class)
      .set(DriverConfiguration.PROGRESS_PROVIDER, JobDriver.ProgressProvider.class)
      .build();

  /**
   * The HTTP Server configuration assumed by the bridge.
   */
  public static final Configuration HTTP_SERVER_CONFIGURATION = Configurations.merge(
      HttpHandlerConfiguration.CONF
          .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
          // Add the http status handler.
          .set(HttpHandlerConfiguration.HTTP_HANDLERS, DriverStatusHTTPHandler.class)
          .build(),
      DriverServiceConfiguration.CONF
          .set(DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED,
              ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
          .set(DriverServiceConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
          .set(DriverServiceConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
          .set(DriverServiceConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
          .set(DriverServiceConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
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
      TANG.newConfigurationBuilder()
          .bindImplementation(JobStatusHandler.class, DriverStatusHTTPHandler.class)
          .build()
  );

  /**
   * The name server configuration assumed by the bridge.
   */
  public static final Configuration NAME_SERVER_CONFIGURATION = NameServerConfiguration.CONF
      .set(NameServerConfiguration.NAME_SERVICE_PORT, 0)
      .build();

  /**
   * The driver configuration assumed by the the bridge.
   */
  public static final Configuration DRIVER_CONFIGURATION_WITH_HTTP_AND_NAMESERVER = Configurations.merge(
      DRIVER_CONFIGURATION,
      HTTP_SERVER_CONFIGURATION,
      NAME_SERVER_CONFIGURATION
  );

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Constants() {
  }
}
