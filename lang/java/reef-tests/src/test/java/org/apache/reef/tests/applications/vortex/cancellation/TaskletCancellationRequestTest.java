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

package org.apache.reef.tests.applications.vortex.cancellation;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.vortex.driver.VortexJobConf;
import org.apache.reef.vortex.driver.VortexMasterConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the cancellation of a tasklet.
 */
public final class TaskletCancellationRequestTest {
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

  @Test
  public void testVortexTaskletCancellation() {
    final Configuration vortexMasterConf = VortexMasterConf.CONF
        .set(VortexMasterConf.WORKER_NUM, 2)
        .set(VortexMasterConf.WORKER_MEM, 64)
        .set(VortexMasterConf.WORKER_CORES, 4)
        .set(VortexMasterConf.WORKER_CAPACITY, 2000)
        .set(VortexMasterConf.VORTEX_START, TaskletCancellationTestStart.class)
        .build();

    final VortexJobConf vortexJobConf = VortexJobConf.newBuilder()
        .setJobName("TEST_Vortex_TaskletCancellationTest")
        .setVortexMasterConf(vortexMasterConf)
        .build();

    final LauncherStatus status = this.testEnvironment.run(vortexJobConf.getConfiguration());
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
