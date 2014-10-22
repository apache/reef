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
package com.microsoft.reef.tests.taskcounting;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.library.driver.OnDriverStartedAllocateOne;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the submission of multiple tasks to an Evaluator in sequence.
 */
public final class TaskCountingTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TaskCounting")
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, TaskCountingDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, TaskCountingDriver.TaskRunningHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, TaskCountingDriver.TaskCompletedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, TaskCountingDriver.DriverStopHandler.class)
        .build();
  }

  @Test
  public void testTaskCounting() throws InjectionException {
    final LauncherStatus state = DriverLauncher.getLauncher(this.testEnvironment.getRuntimeConfiguration())
        .run(getDriverConfiguration());

    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
  }
}
