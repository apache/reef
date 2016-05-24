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
package org.apache.reef.tests;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.yarn.failure.FailureREEF;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * REEF failure test.
 */
public class FailureTest {

  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testSingleEvaluatorFailureAndRestart() throws InjectionException {
    runTestFailureReefWithParams(1, 1, "testSingleEvaluatorFailureAndRestart");
  }

  @Test
  public void testFailureRestart() throws InjectionException {
    runTestFailureReefWithParams(40, 10, "testFailureRestart");
  }

  private void runTestFailureReefWithParams(final int numEvaluatorsToSubmit,
                                            final int numEvaluatorsTofail,
                                            final String testName) throws InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final LauncherStatus status =
        FailureREEF.runFailureReef(runtimeConfiguration, this.testEnvironment.getTestTimeout(),
            numEvaluatorsToSubmit, numEvaluatorsTofail);

    Assert.assertTrue("FailureReef " + testName + " failed: " + status, status.isSuccess());
  }
}
