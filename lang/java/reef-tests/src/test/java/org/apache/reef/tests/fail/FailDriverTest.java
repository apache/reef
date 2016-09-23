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

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.SuspendedTask;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.TestUtils;
import org.apache.reef.tests.fail.driver.FailClient;
import org.apache.reef.tests.fail.driver.FailDriver;
import org.apache.reef.tests.library.exceptions.SimulatedDriverFailure;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Run FailDriver with different types of failures.
 */
public class FailDriverTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private void failOn(final Class<?> clazz) throws BindException, InjectionException {
    TestUtils.assertLauncherFailure(
        FailClient.runClient(clazz,
            this.testEnvironment.getRuntimeConfiguration(), this.testEnvironment.getTestTimeout()),
        SimulatedDriverFailure.class);
  }

  @Test
  public void testFailDriverConstructor() throws BindException, InjectionException {
    failOn(FailDriver.class);
  }

  @Test
  public void testFailDriverStart() throws BindException, InjectionException {
    failOn(StartTime.class);
  }

  @Test
  public void testFailDriverAllocatedEvaluator() throws BindException, InjectionException {
    failOn(AllocatedEvaluator.class);
  }

  @Test
  public void testFailDriverActiveContext() throws BindException, InjectionException {
    failOn(ActiveContext.class);
  }

  @Test
  public void testFailDriverRunningTask() throws BindException, InjectionException {
    failOn(RunningTask.class);
  }

  @Test
  public void testFailDriverTaskMessage() throws BindException, InjectionException {
    failOn(TaskMessage.class);
  }

  @Test
  public void testFailDriverSuspendedTask() throws BindException, InjectionException {
    failOn(SuspendedTask.class);
  }

  @Test
  public void testFailDriverCompletedTask() throws BindException, InjectionException {
    failOn(CompletedTask.class);
  }

  @Test
  public void testFailDriverCompletedEvaluator() throws BindException, InjectionException {
    failOn(CompletedEvaluator.class);
  }

  @Test
  public void testFailDriverAlarm() throws BindException, InjectionException {
    failOn(Alarm.class);
  }

  @Test
  public void testFailDriverStop() throws BindException, InjectionException {
    failOn(StopTime.class);
  }

  @Test
  public void testDriverCompleted() throws BindException, InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();
    // FailDriverTest can be replaced with any other class never used in FailDriver
    final LauncherStatus status = FailClient.runClient(
        FailDriverTest.class, runtimeConfiguration, this.testEnvironment.getTestTimeout());
    Assert.assertEquals(LauncherStatus.COMPLETED, status);
  }
}
