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
package org.apache.reef.examples.hellohttp;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.DriverServiceConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.HttpServerReefEventHandler;
import org.apache.reef.webserver.ReefEventStateManager;

/**
 * Distributed shell example based on REEF HTTP Server component.
 */
public final class HelloREEFHttp {
  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 3;

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  public static final int JOB_TIMEOUT = 60 * 1000; // 60 sec.

  /**
   * @return the driver-side configuration to be merged into the DriverConfiguration to enable the HTTP server.
   */
  public static Configuration getHTTPConfiguration() {
    final Configuration httpHandlerConfiguration = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerShellCmdHandler.class)
        .build();
    final Configuration driverConfigurationForHttpServer = DriverServiceConfiguration.CONF
        .set(DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED,
            ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
        .set(DriverServiceConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
        .set(DriverServiceConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
        .set(DriverServiceConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
        .build();
    return Configurations.merge(httpHandlerConfiguration, driverConfigurationForHttpServer);
  }

  /**
   * @return the configuration of the HelloREEF driver.
   */
  public static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HttpShellJobDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HttpShellJobDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HttpShellJobDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, HttpShellJobDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, HttpShellJobDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, HttpShellJobDriver.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, HttpShellJobDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, HttpShellJobDriver.CompletedTaskHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, HttpShellJobDriver.ClientMessageHandler.class)
        .set(DriverConfiguration.ON_CLIENT_CLOSED, HttpShellJobDriver.HttpClientCloseHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, HttpShellJobDriver.StopHandler.class)
        .build();
  }

  /**
   * Run Hello Reef with merged configuration.
   *
   * @param runtimeConf
   * @param timeOut
   * @return
   * @throws BindException
   * @throws InjectionException
   */
  public static LauncherStatus runHelloReef(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {
    final Configuration driverConf =
        Configurations.merge(HelloREEFHttp.getDriverConfiguration(), getHTTPConfiguration());
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

  /**
   * Main program.
   *
   * @param args
   * @throws InjectionException
   */
  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
    runHelloReef(runtimeConfiguration, HelloREEFHttp.JOB_TIMEOUT);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private HelloREEFHttp() {
  }
}
