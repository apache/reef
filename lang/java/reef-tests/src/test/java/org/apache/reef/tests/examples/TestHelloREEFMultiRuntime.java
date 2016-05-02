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
package org.apache.reef.tests.examples;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.hellomultiruntime.HelloMultiRuntimeDriver;
import org.apache.reef.runtime.multi.client.MultiRuntimeConfigurationBuilder;
import org.apache.reef.runtime.yarn.driver.RuntimeIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.YarnTestEnvironment;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests whether the HelloREEF example runs successfully.
 */
public class TestHelloREEFMultiRuntime {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testHelloREEFMultiRuntime() throws InjectionException {
    if(this.testEnvironment instanceof YarnTestEnvironment){
      // multi runtime can be tested on yarn only
      final Configuration driverConf = DriverConfiguration.CONF
              .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
              .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_HelloREEFMultiRunitme")
              .set(DriverConfiguration.ON_DRIVER_STARTED, HelloMultiRuntimeDriver.StartHandler.class)
              .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloMultiRuntimeDriver.EvaluatorAllocatedHandler.class)
              .build();

      // create the multi runtime environment
      Configuration multiruntimeConfig = new MultiRuntimeConfigurationBuilder()
              .addRuntime(RuntimeIdentifier.RUNTIME_NAME)
              .addRuntime(org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME)
              .setDefaultRuntime(RuntimeIdentifier.RUNTIME_NAME)
              .setMaxEvaluatorsNumberForLocalRuntime(1)
              .setSubmissionRuntime(RuntimeIdentifier.RUNTIME_NAME)
              .build();

      final LauncherStatus state = DriverLauncher.getLauncher(multiruntimeConfig).run(driverConf, this
              .testEnvironment.getTestTimeout());
      Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
    }
  }

}
