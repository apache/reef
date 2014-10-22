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
package com.microsoft.reef.tests.fail;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.tests.TestDriverLauncher;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.TestUtils;
import com.microsoft.reef.tests.fail.driver.DriverFailOnFail;
import com.microsoft.reef.tests.library.exceptions.SimulatedDriverFailure;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Run FailDriver with different types of failures.
 */
public final class DriverFailOnFailTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testDriverFailOnFail() throws BindException, InjectionException {

    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "Fail2")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, DriverFailOnFail.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, DriverFailOnFail.FailedTaskHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, DriverFailOnFail.StartHandler.class)
        .build();

    TestUtils.assertLauncherFailure(
        TestDriverLauncher.getLauncher(runtimeConfiguration).run(
            driverConfig, this.testEnvironment.getTestTimeout()),
        SimulatedDriverFailure.class);
  }
}
