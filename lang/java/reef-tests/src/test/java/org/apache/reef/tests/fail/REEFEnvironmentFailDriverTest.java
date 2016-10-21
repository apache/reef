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

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.SuspendedTask;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.parameters.ClientRemoteIdentifier;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.runtime.local.driver.LocalDriverConfiguration;
import org.apache.reef.runtime.local.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestUtils;
import org.apache.reef.tests.fail.driver.FailClient;
import org.apache.reef.tests.fail.driver.FailDriver;
import org.apache.reef.tests.library.exceptions.SimulatedDriverFailure;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.junit.Assert;
import org.junit.Test;

/**
 * Run FailDriver with different types of failures using in-process driver with local runtime.
 */
public class REEFEnvironmentFailDriverTest {

  private static final Configuration LOCAL_DRIVER_MODULE = LocalDriverConfiguration.CONF
      .set(LocalDriverConfiguration.RUNTIME_NAMES, RuntimeIdentifier.RUNTIME_NAME)
      .set(LocalDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, ClientRemoteIdentifier.NONE)
      .set(LocalDriverConfiguration.JOB_IDENTIFIER, "LOCAL_ENV_FAIL_DRIVER_TEST")
      .set(LocalDriverConfiguration.ROOT_FOLDER, "./REEF_LOCAL_RUNTIME")
      .set(LocalDriverConfiguration.MAX_NUMBER_OF_EVALUATORS, 1)
      .set(LocalDriverConfiguration.JVM_HEAP_SLACK, 0.0)
      .build();

  private static void failOn(final Class<?> clazz) throws BindException, InjectionException {

    final Injector injector = Tang.Factory.getTang().newInjector(LOCAL_DRIVER_MODULE);
    final ExceptionCodec exceptionCodec = injector.getInstance(ExceptionCodec.class);

    TestUtils.assertJobFailure(
        FailClient.runInProcess(clazz, LOCAL_DRIVER_MODULE, 0),
        exceptionCodec, SimulatedDriverFailure.class);
  }

  @Test
  public void testFailDriverConstructor() throws BindException, InjectionException {
    try {
      failOn(FailDriver.class);
    } catch (final Throwable ex) {
      Assert.assertTrue("Unexpected error: " + ex, TestUtils.hasCause(ex, SimulatedDriverFailure.class));
    }
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

    // REEFEnvironmentFailDriverTest can be replaced with any other class never used in FailDriver
    final ReefServiceProtos.JobStatusProto status = FailClient.runInProcess(
        REEFEnvironmentFailDriverTest.class, LOCAL_DRIVER_MODULE, 0);

    Assert.assertNotNull("Final job status must not be null", status);
    Assert.assertTrue("Job state missing", status.hasState());
    Assert.assertEquals("Unexpected final job state", ReefServiceProtos.State.DONE, status.getState());
  }
}
