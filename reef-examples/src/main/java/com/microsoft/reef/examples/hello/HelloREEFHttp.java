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
package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.common.driver.defaults.DefaultContextActiveHandler;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.reef.webserver.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example to run HelloREEF with a webserver.
 */
public final class HelloREEFHttp {
    private static final Logger LOG = Logger.getLogger(HelloREEFHttp.class.getName());

    /**
     * Number of milliseconds to wait for the job to complete.
     */
    public static final int JOB_TIMEOUT = 1000000; // 1000 sec.


  /**
   * @return the driver-side configuration to be merged into the DriverConfiguration to enable the HTTP server.
   */
  public static Configuration getHTTPConfiguration() {
    return HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
  }

  /**
   * @return the configuration of the HelloREEF driver.
   */
  public static Configuration getDriverConfiguration() {
      return EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DefaultContextActiveHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
        .set(DriverConfiguration.ON_CLIENT_MESSAGE, ReefEventStateManager.ClientMessageStateHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
        .build();
  }

    /**
     * Run Hello Reef with merged configuration
     * @param runtimeConf
     * @param timeOut
     * @return
     * @throws BindException
     * @throws InjectionException
     */
  public static LauncherStatus runHelloReef(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {
    final Configuration driverConf = Configurations.merge(HelloREEFHttp.getDriverConfiguration(), getHTTPConfiguration());
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

    /**
     * main program
     * @param args
     * @throws InjectionException
     */
  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    final LauncherStatus status = runHelloReef(runtimeConfiguration, HelloREEFHttp.JOB_TIMEOUT);
  }
}