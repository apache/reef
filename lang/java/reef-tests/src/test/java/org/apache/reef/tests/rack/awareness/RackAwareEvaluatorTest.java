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
package org.apache.reef.tests.rack.awareness;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.LocalTestEnvironment;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.library.driver.OnDriverStartedAllocateOne;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests whether an Evaluator receives its rack information.
 * The available racks can be configured in the local runtime.
 */
public final class RackAwareEvaluatorTest {

  private static final String RACK1 = "rack1";
  // runs on the local runtime
  private final TestEnvironment testEnvironment = new LocalTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  /**
  * Tests whether the runtime passes the rack information to the driver
  * The success scenario is if it receives the default rack, fails otherwise
  */
  @Test
  public void testRackAwareEvaluatorRunningOnDefaultRack() {
    //Given
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_RackAwareEvaluator")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(RackAwareEvaluatorTestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, RackAwareEvaluatorTestDriver.EvaluatorAllocatedHandler.class)
        .build();

    // When
    final LauncherStatus status = this.testEnvironment.run(driverConfiguration);
    // Then
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }

  /**
   * Test whether the runtime passes the rack information to the driver
   * The success scenario is if it receives rack1, fails otherwise
   */
  //@Test
  // TODO Re-enable once we define the API to specify the information where resources should run on
  // OnDriverStartedAllocateOne will need to be replaced, and contain that it wants to run in RACK1, which will be the only one available
  public void testRackAwareEvaluatorRunningOnRack1() throws InjectionException {
    //Given
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_RackAwareEvaluator")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(RackAwareEvaluatorTestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, RackAwareEvaluatorTestDriver.EvaluatorAllocatedHandler.class)
        .build();

    // update the drive config with the rack to assert on
    final Configuration testDriverConfig = Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration)
        .bindNamedParameter(RackNameParameter.class, RACK1).build();

    // update the runtime config with the rack available using the config module
    final Configuration testRuntimeConfig = Tang.Factory.getTang()
        .newConfigurationBuilder(this.testEnvironment.getRuntimeConfiguration()).bindSetEntry(RackNames.class, RACK1)
        .build();

    // When
    final LauncherStatus status = DriverLauncher.getLauncher(testRuntimeConfig)
                                              .run(testDriverConfig, this.testEnvironment.getTestTimeout());
    // Then
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }



}
