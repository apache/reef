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
package org.apache.reef.tests.group;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.group.impl.driver.GroupCommService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Launch Group Communication test using multiple communication groups.
 */
public class TestMultipleCommGroups {
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
   * Run the MultipleCommGroups test.
   */
  @Test
  public void testMultipleCommGroups() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MultipleCommGroupsDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_MultipleCommGroups")
        .set(DriverConfiguration.ON_DRIVER_STARTED, MultipleCommGroupsDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MultipleCommGroupsDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MultipleCommGroupsDriver.ContextActiveHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, MultipleCommGroupsDriver.TaskRunningHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, MultipleCommGroupsDriver.TaskCompletedHandler.class)
        .build();
    final Configuration groupCommConf = GroupCommService.getConfiguration();
    final LauncherStatus state = this.testEnvironment.run(Configurations.merge(driverConf, groupCommConf));
    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
  }
}
