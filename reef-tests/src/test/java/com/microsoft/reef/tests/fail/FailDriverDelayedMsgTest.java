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
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestDriverLauncher;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.fail.driver.FailDriverDelayedMsg;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Run FailDriver with different types of failures.
 */
public class FailDriverDelayedMsgTest {

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
  public void testFailDriverTestMessage() throws BindException, InjectionException {

    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfig = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "FailDriverDelayedMsg")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FailDriverDelayedMsg.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, FailDriverDelayedMsg.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, FailDriverDelayedMsg.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, FailDriverDelayedMsg.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, FailDriverDelayedMsg.StartHandler.class)
        .build();

    final LauncherStatus status = TestDriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfig, this.testEnvironment.getTestTimeout());

    Assert.assertEquals(LauncherStatus.COMPLETED, status);
  }
}
