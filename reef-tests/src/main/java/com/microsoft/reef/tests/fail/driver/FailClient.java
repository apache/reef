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
package com.microsoft.reef.tests.fail.driver;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestDriverLauncher;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Client for the test REEF job that fails on different stages of execution.
 */
public final class FailClient {

  public static LauncherStatus run(final Class<?> failMsgClass,
                                   final Configuration runtimeConfig,
                                   final int timeOut) throws BindException, InjectionException {

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

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(driverConfig);
    cb.bindNamedParameter(FailDriver.FailMsgClassName.class, failMsgClass.getName());

    return TestDriverLauncher.getLauncher(runtimeConfig).run(cb.build(), timeOut);
  }
}
