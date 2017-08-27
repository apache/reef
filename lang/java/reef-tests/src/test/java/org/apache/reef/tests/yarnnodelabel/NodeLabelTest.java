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
package org.apache.reef.tests.yarnnodelabel;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.*;

/**
 * Tests whether Evaluator allocations requested with a given node label are fulfilled.
 * Before running this test, configure you YARN cluster to use two partition, "default" and "mylabel".
 * Refer to https://issues.apache.org/jira/browse/REEF-1750.
 *
 * Application manager starts on the default partition. Then the NodeLabelTestDriver submits an EvaluatorRequest
 * labeled with "mylabel" and one without label. The test passes if there comes one evaluator from "mylabel" node,
 * and another from "default" node.
 */
public class NodeLabelTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private LauncherStatus runNodeLabelTest() throws BindException, InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(NodeLabelTestDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_REEFYarnNodeLabelTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, NodeLabelTestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, NodeLabelTestDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, NodeLabelTestDriver.StopHandler.class)
        .build();

    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(driverConfiguration, this.testEnvironment.getTestTimeout());
    return state;
  }


  @Test
  public void testNodeLabel() throws BindException, InjectionException {
    Assume.assumeTrue("This test requires a YARN Resource Manager to connect to",
        Boolean.parseBoolean(System.getenv("REEF_TEST_YARN")));

    final LauncherStatus state = runNodeLabelTest();
    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
  }
}
