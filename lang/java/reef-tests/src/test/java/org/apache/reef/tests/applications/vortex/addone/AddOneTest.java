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
package org.apache.reef.tests.applications.vortex.addone;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.vortex.driver.VortexConfHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Launch the AddOne Vortex test.
 */
public final class AddOneTest {
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

  /**
   * Run the AddOne test.
   */
  @Test
  public void testVortexAddOne() {
    final Configuration conf =
        VortexConfHelper.getVortexConf("TEST_Vortex_AddOneTest", AddOneTestStart.class, 2, 64, 4, 2000);
    final LauncherStatus status = this.testEnvironment.run(conf);
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
