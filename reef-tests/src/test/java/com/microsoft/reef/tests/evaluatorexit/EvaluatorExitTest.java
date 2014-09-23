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
package com.microsoft.reef.tests.evaluatorexit;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.library.driver.OnDriverStartedAllocateOne;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

/**
 * Tests whether an unexpected Evaluator exit (via System.exit()) is reported as a FailedEvaluator to the Driver.
 */
public final class EvaluatorExitTest {
  private static final Logger LOG = Logger.getLogger(EvaluatorExitTest.class.getName());
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
  public void testEvaluatorExit() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_EvaluatorExit")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(EvaluatorExitTestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, EvaluatorExitTestDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, EvaluatorExitTestDriver.EvaluatorFailureHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, EvaluatorExitTestDriver.StopHandler.class)
        .build();
    final LauncherStatus status = this.testEnvironment.run(driverConfiguration);
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }

}
