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

package org.apache.reef.tests.fail.driver;


import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.DriverClientConfiguration;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestDriverLauncher;
import org.apache.reef.tests.fail.util.FailBridgeClientUtils;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.OSUtils;

import java.io.IOException;

/**
 * fail bridge client.
 */
@Private
@ClientSide
public final class FailBridgeClient {

  private static final Tang TANG = Tang.Factory.getTang();

  private static Configuration buildDriverConfig(final Class<?> failMsgClass) {

    final Configuration driverConfig = DriverClientConfiguration.CONF
        .set(DriverClientConfiguration.ON_DRIVER_STARTED, FailDriver.StartHandler.class)
        .set(DriverClientConfiguration.ON_DRIVER_STOP, FailDriver.StopHandler.class)
        .set(DriverClientConfiguration.ON_EVALUATOR_ALLOCATED, FailDriver.AllocatedEvaluatorHandler.class)
        .set(DriverClientConfiguration.ON_EVALUATOR_COMPLETED, FailDriver.CompletedEvaluatorHandler.class)
        .set(DriverClientConfiguration.ON_EVALUATOR_FAILED, FailDriver.FailedEvaluatorHandler.class)
        .set(DriverClientConfiguration.ON_CONTEXT_ACTIVE, FailDriver.ActiveContextHandler.class)
        .set(DriverClientConfiguration.ON_CONTEXT_MESSAGE, FailDriver.ContextMessageHandler.class)
        .set(DriverClientConfiguration.ON_CONTEXT_CLOSED, FailDriver.ClosedContextHandler.class)
        .set(DriverClientConfiguration.ON_CONTEXT_FAILED, FailDriver.FailedContextHandler.class)
        .set(DriverClientConfiguration.ON_TASK_RUNNING, FailDriver.RunningTaskHandler.class)
        .set(DriverClientConfiguration.ON_TASK_SUSPENDED, FailDriver.SuspendedTaskHandler.class)
        .set(DriverClientConfiguration.ON_TASK_MESSAGE, FailDriver.TaskMessageHandler.class)
        .set(DriverClientConfiguration.ON_TASK_FAILED, FailDriver.FailedTaskHandler.class)
        .set(DriverClientConfiguration.ON_TASK_COMPLETED, FailDriver.CompletedTaskHandler.class)
        .build();

    return TANG.newConfigurationBuilder(driverConfig)
        .bindNamedParameter(FailDriver.FailMsgClassName.class, failMsgClass.getName())
        .build();
  }

  public static LauncherStatus runClient(
      final Class<?> failMsgClass,
      final Configuration runtimeConfig,
      final int timeOut) throws IOException, InjectionException {
    ClientProtocol.DriverClientConfiguration.Builder builder =
        ClientProtocol.DriverClientConfiguration.newBuilder()
        .setJobid("Fail_" + failMsgClass.getSimpleName())
        .addGlobalLibraries(EnvironmentUtils.getClassLocation(FailDriver.class));
    builder.setOperatingSystem(
        OSUtils.isWindows() ?
            ClientProtocol.DriverClientConfiguration.OS.WINDOWS :
            ClientProtocol.DriverClientConfiguration.OS.LINUX);

    return runClient(failMsgClass, runtimeConfig, builder.build(), timeOut);
  }

  public static LauncherStatus runClient(
      final Class<?> failMsgClass,
      final Configuration runtimeConfig,
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration,
      final int timeOut) throws InjectionException, IOException {
    final Configuration driverServiceConfiguration =
        FailBridgeClientUtils.setupDriverService(
            runtimeConfig,
            buildDriverConfig(failMsgClass),
            driverClientConfiguration);
    return TestDriverLauncher.getLauncher(runtimeConfig).run(driverServiceConfiguration, timeOut);
  }


  private FailBridgeClient() {
  }
}
