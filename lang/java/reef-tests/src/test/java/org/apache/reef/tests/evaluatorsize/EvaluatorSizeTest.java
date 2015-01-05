/**
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
package org.apache.reef.tests.evaluatorsize;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests whether Evaluator allocations requested with a given amount of memory are (over-)fullfilled.
 */
public class EvaluatorSizeTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private LauncherStatus runEvaluatorSizeTest(final int megaBytes) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration testConfiguration = EvaluatorSizeTestConfiguration.CONF
        .set(EvaluatorSizeTestConfiguration.MEMORY_SIZE, 777)
        .build();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_EvaluatorSizeTest-" + megaBytes)
        .set(DriverConfiguration.ON_DRIVER_STARTED, EvaluatorSizeTestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, EvaluatorSizeTestDriver.EvaluatorAllocatedHandler.class).build();

    final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
        .newConfigurationBuilder(driverConfiguration, testConfiguration).build();

    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(mergedDriverConfiguration, this.testEnvironment.getTestTimeout());
    return state;
  }


  @Test
  public void testEvaluatorSize() throws BindException, InjectionException {
    final LauncherStatus state = runEvaluatorSizeTest(777);
    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
  }
}
