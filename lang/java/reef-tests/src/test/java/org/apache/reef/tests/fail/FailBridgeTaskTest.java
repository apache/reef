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
package org.apache.reef.tests.fail;

import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.TestUtils;
import org.apache.reef.tests.fail.task.*;
import org.apache.reef.tests.library.exceptions.SimulatedTaskFailure;
import org.apache.reef.util.OSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Run Driver with different types of failures in the Task.
 */
public final class FailBridgeTaskTest {

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
      final Class<? extends Task> failTaskClass) throws BindException, InjectionException, IOException {
    if (OSUtils.isLinux()) {
      TestUtils.assertLauncherFailure(
          BridgeClient.run(failTaskClass,
              this.testEnvironment.getRuntimeConfiguration(),
              2 * this.testEnvironment.getTestTimeout()),
          SimulatedTaskFailure.class);
    }
  }

  @Test
  public void testFailTask() throws BindException, InjectionException, IOException {
    failOn(FailTask.class);
  }

  @Test
  public void testFailTaskCall() throws BindException, InjectionException, IOException {
    failOn(FailTaskCall.class);
  }

  @Test
  public void testFailTaskMsg() throws BindException, InjectionException, IOException {
    failOn(FailTaskMsg.class);
  }

  @Test
  public void testFailTaskSuspend() throws BindException, InjectionException, IOException {
    failOn(FailTaskSuspend.class);
  }

  @Test
  public void testFailTaskStart() throws BindException, InjectionException, IOException {
    failOn(FailTaskStart.class);
  }

  @Test
  public void testFailTaskStop() throws BindException, InjectionException, IOException {
    failOn(FailTaskStop.class);
  }

  @Test
  public void testFailTaskClose() throws BindException, InjectionException, IOException {
    failOn(FailTaskClose.class);
  }
}
