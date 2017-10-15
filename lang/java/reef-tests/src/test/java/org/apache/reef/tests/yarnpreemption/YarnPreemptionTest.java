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
package org.apache.reef.tests.yarnpreemption;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.*;

/**
 * Tests whether REEF can handle PreemptedEvaluator when a YARN container is preempted.
 */
public class YarnPreemptionTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private void runYarnPreemptionTest() throws InjectionException, InterruptedException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    Thread preempteeThread = new Thread() {
      public void run() {

        final Configuration testConfigurationB = YarnPreemptionTestConfiguration.CONF
            .set(YarnPreemptionTestConfiguration.JOB_QUEUE, "B")
            .build();

        final Configuration driverConfiguration = DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_YarnPreemptionTest_Preemptee")
            .set(DriverConfiguration.ON_DRIVER_STARTED, YarnPreemptionTestPreempteeDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
                YarnPreemptionTestPreempteeDriver.EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_PREEMPTED,
                YarnPreemptionTestPreempteeDriver.EvaluatorPreemptedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED,
                YarnPreemptionTestPreempteeDriver.EvaluatorFailedHandler.class)
            .build();


        final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
            .newConfigurationBuilder(driverConfiguration, testConfigurationB).build();

        try {
          final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
              .run(mergedDriverConfiguration, testEnvironment.getTestTimeout());
          Assert.assertTrue("Job B (preemptee) state after execution: " + state, state.isDone());

        } catch (InjectionException e) {
          e.printStackTrace();
        }
      }
    };

    Thread preemptorThread = new Thread() {
      public void run() {

        try {
          Thread.sleep(6000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        final Configuration testConfigurationA = YarnPreemptionTestConfiguration.CONF
            .set(YarnPreemptionTestConfiguration.JOB_QUEUE, "A")
            .build();

        final Configuration driverConfiguration = DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(this.getClass()))
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_YarnPreemptionTest_Preemptor")
            .set(DriverConfiguration.ON_DRIVER_STARTED, YarnPreemptionTestPreemptorDriver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
                YarnPreemptionTestPreemptorDriver.EvaluatorAllocatedHandler.class)
            .build();

        final Configuration mergedDriverConfiguration = Tang.Factory.getTang()
            .newConfigurationBuilder(driverConfiguration, testConfigurationA).build();

        try {
          final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
              .run(mergedDriverConfiguration, testEnvironment.getTestTimeout());
          Assert.assertTrue("Job A (preemptor) state after execution: " + state, state.isSuccess());
        } catch (InjectionException e) {
          e.printStackTrace();
        }

      }
    };

    preempteeThread.start();
    preemptorThread.start();

    preempteeThread.join();
    preemptorThread.join();
  }

  @Test
  public void testYarnPreemption() throws InjectionException, InterruptedException {
    Assume.assumeTrue("This test requires a YARN Resource Manager to connect to",
        Boolean.parseBoolean(System.getenv("REEF_TEST_YARN")));
    runYarnPreemptionTest();
  }
}
