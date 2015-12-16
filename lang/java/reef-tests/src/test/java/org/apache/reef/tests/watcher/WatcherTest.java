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
package org.apache.reef.tests.watcher;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.watcher.LogEventStream;
import org.apache.reef.io.watcher.WatcherConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.time.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public final class WatcherTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  /**
   * Set up the test environment.
   */
  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  /**
   * Tear down the test environment.
   */
  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private Configuration getDriverConfiguration() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "WatcherTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, WatcherTestDriver.DriverStartedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, WatcherTestDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, WatcherTestDriver.EvaluatorFailedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, WatcherTestDriver.ContextActivatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, WatcherTestDriver.ContextFailedHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, WatcherTestDriver.TaskFailedHandler.class)
        .build();

    final Configuration runtimeStopConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(Clock.RuntimeStopHandler.class, WatcherTestDriver.RuntimeStopHandler.class)
        .build();

    return Configurations.merge(driverConf, runtimeStopConf, WatcherConfiguration.CONF
        .set(WatcherConfiguration.EVENT_STREAMS, LogEventStream.class)
        .set(WatcherConfiguration.EVENT_STREAMS, TestEventStream.class)
        .build());
  }

  @Test
  public void testEventStream() {
    final LauncherStatus status = this.testEnvironment.run(getDriverConfiguration());
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
