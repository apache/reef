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
package com.microsoft.reef.tests.close_eval;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests evaluator allocation by asking for allocations that it immediately closes.
 */
public class CloseEvaluatorTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @Test
  public void testCloseEvaluator() throws BindException, InjectionException {

    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(CloseEvaluatorDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_CloseEvaluatorTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, CloseEvaluatorDriver.StartHandler.class)
        .build();

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }
}
