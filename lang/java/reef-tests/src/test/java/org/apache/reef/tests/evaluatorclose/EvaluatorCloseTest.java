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
package org.apache.reef.tests.evaluatorclose;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.library.driver.OnDriverStartedAllocateOne;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests whether evaluator's state is properly changed during closing.
 */
public class EvaluatorCloseTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  /**
   * Tests that an evaluator's state is changed to closing and closed in order during closing.
   */
  @Test
  public void testEvaluatorClosingState() throws InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            EnvironmentUtils.getClassLocation(EvaluatorCloseDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_EvaluatorCloseTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, EvaluatorCloseDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, EvaluatorCloseDriver.TaskRunningHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, EvaluatorCloseDriver.StopHandler.class)
        .build();

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());

    Assert.assertTrue("The result state of evaluator closing state test is " + status,
        status.isSuccess());
  }
}
