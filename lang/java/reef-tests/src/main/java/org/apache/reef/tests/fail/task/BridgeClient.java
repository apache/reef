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

package org.apache.reef.tests.fail.task;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.DriverClientConfiguration;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.tests.TestDriverLauncher;
import org.apache.reef.tests.fail.util.FailBridgeClientUtils;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.OSUtils;

import java.io.IOException;

/**
 * Fail task bridge client.
 */
@Private
@ClientSide
public final class BridgeClient  {

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private BridgeClient() {
  }

  public static LauncherStatus run(
      final Class<? extends Task> failTaskClass,
      final Configuration runtimeConfig,
      final int timeOut) throws IOException, InjectionException {
    ClientProtocol.DriverClientConfiguration.Builder builder =
        ClientProtocol.DriverClientConfiguration.newBuilder()
            .setJobid("Fail_" + failTaskClass.getSimpleName())
            .addGlobalLibraries(EnvironmentUtils.getClassLocation(Driver.class));
    builder.setOperatingSystem(
        OSUtils.isWindows() ?
            ClientProtocol.DriverClientConfiguration.OS.WINDOWS :
            ClientProtocol.DriverClientConfiguration.OS.LINUX);

    return run(failTaskClass, runtimeConfig, builder.build(), timeOut);
  }

  public static LauncherStatus run(
      final Class<? extends Task> failTaskClass,
      final Configuration runtimeConfig,
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration,
      final int timeOut) throws InjectionException, IOException {

    final Configuration driverConfig = DriverClientConfiguration.CONF
        .set(DriverClientConfiguration.ON_EVALUATOR_ALLOCATED, Driver.AllocatedEvaluatorHandler.class)
        .set(DriverClientConfiguration.ON_TASK_RUNNING, Driver.RunningTaskHandler.class)
        .set(DriverClientConfiguration.ON_CONTEXT_ACTIVE, Driver.ActiveContextHandler.class)
        .set(DriverClientConfiguration.ON_DRIVER_STARTED, Driver.StartHandler.class)
        .build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(driverConfig);
    cb.bindNamedParameter(Driver.FailTaskName.class, failTaskClass.getSimpleName());

    final Configuration driverServiceConfiguration =
        FailBridgeClientUtils.setupDriverService(
            runtimeConfig,
            cb.build(),
            driverClientConfiguration);
    return TestDriverLauncher.getLauncher(runtimeConfig).run(driverServiceConfiguration, timeOut);
  }
}
