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
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.common.REEFEnvironment;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestDriverLauncher;
import org.apache.reef.util.EnvironmentUtils;

/**
 * Client for the test REEF job that fails on different stages of execution.
 */
@Private
@ClientSide
public final class FailClient {

  private static Configuration buildDriverConfig(final Class<?> failMsgClass) {

    final Configuration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(FailDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "Fail_" + failMsgClass.getSimpleName())
        .set(DriverConfiguration.ON_DRIVER_STARTED, FailDriver.StartHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, FailDriver.StopHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FailDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, FailDriver.CompletedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, FailDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, FailDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_MESSAGE, FailDriver.ContextMessageHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_CLOSED, FailDriver.ClosedContextHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, FailDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, FailDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_SUSPENDED, FailDriver.SuspendedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, FailDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, FailDriver.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, FailDriver.CompletedTaskHandler.class)
        .build();

    return Tang.Factory.getTang().newConfigurationBuilder(driverConfig)
        .bindNamedParameter(FailDriver.FailMsgClassName.class, failMsgClass.getName())
        .build();
  }

  /**
   * Run REEF on specified runtime and fail (raise an exception) in a specified class.
   * @param failMsgClass A class that should fail during the test.
   * @param runtimeConfig REEF runtime configuration. Can be e.g. Local or YARN.
   * @param timeOut REEF application timeout.
   * @return launcher status - usually FAIL.
   * @throws InjectionException configuration error.
   */
  public static LauncherStatus runClient(final Class<?> failMsgClass,
      final Configuration runtimeConfig, final int timeOut) throws InjectionException {

    return TestDriverLauncher.getLauncher(runtimeConfig).run(buildDriverConfig(failMsgClass), timeOut);
  }

  /**
   * Run REEF in-process using specified runtime and fail (raise an exception) in a specified class.
   * @param failMsgClass A class that should fail during the test.
   * @param runtimeConfig REEF runtime configuration. Can be e.g. Local or YARN.
   * @param timeOut REEF application timeout - not used yet.
   * @return launcher status - usually FAIL.
   * @throws InjectionException configuration error.
   */
  public static LauncherStatus runInProcess(final Class<?> failMsgClass,
      final Configuration runtimeConfig, final int timeOut) throws InjectionException {

    try (final REEFEnvironment reef =
             REEFEnvironment.fromConfiguration(runtimeConfig, buildDriverConfig(failMsgClass))) {
      reef.run();
    }

    return LauncherStatus.FORCE_CLOSED; // TODO[REEF-1596]: Use the actual status, when implemented.
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private FailClient() {
  }
}
