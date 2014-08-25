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

import com.microsoft.reef.task.Task;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.tests.TestUtils;
import com.microsoft.reef.tests.fail.task.*;
import com.microsoft.reef.tests.library.exceptions.SimulatedTaskFailure;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Run Driver with different types of failures in the Task.
 */
public final class FailTaskTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private void failOn(
      final Class<? extends Task> failTaskClass) throws BindException, InjectionException {
    TestUtils.assertLauncherFailure(
        Client.run(failTaskClass,
            this.testEnvironment.getRuntimeConfiguration(),
            this.testEnvironment.getTestTimeout()),
        SimulatedTaskFailure.class);
  }

  @Test
  public void testFailTask() throws BindException, InjectionException {
    failOn(FailTask.class);
  }

  @Test
  public void testFailTaskCall() throws BindException, InjectionException {
    failOn(FailTaskCall.class);
  }

  @Test
  public void testFailTaskMsg() throws BindException, InjectionException {
    failOn(FailTaskMsg.class);
  }

  @Test
  public void testFailTaskSuspend() throws BindException, InjectionException {
    failOn(FailTaskSuspend.class);
  }

  @Test
  public void testFailTaskStart() throws BindException, InjectionException {
    failOn(FailTaskStart.class);
  }

  @Test
  public void testFailTaskStop() throws BindException, InjectionException {
    failOn(FailTaskStop.class);
  }

  @Test
  public void testFailTaskClose() throws BindException, InjectionException {
    failOn(FailTaskClose.class);
  }
}
