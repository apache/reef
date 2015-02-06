/**
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
package org.apache.reef.tests.fail;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestDriverLauncher;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.TestUtils;
import org.apache.reef.tests.fail.driver.DriverFailOnFail;
import org.apache.reef.tests.library.exceptions.SimulatedDriverFailure;
import org.apache.reef.util.EnvironmentUtils;
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
