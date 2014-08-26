/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.examples;

import com.microsoft.reef.examples.retained_eval.JobClient;
import com.microsoft.reef.examples.retained_eval.Launch;
import com.microsoft.reef.tests.LocalTestEnvironment;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An integration test for retained evaluators: Run a simple `echo` on a couple of Evaluators a few times and make sure
 * it comes back.
 */
public final class TestRetainedEvaluators {
  /**
   * Message to print in (remote) shells.
   */
  private static final String MESSAGE = "Hello REEF";

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
  public void testRetainedEvaluators() throws InjectionException {
    final Configuration clientConfiguration = Configurations.merge(
        JobClient.getClientConfiguration(),        // The special job client.
        getLaunchConfiguration(),                  // Specific configuration for this job
        testEnvironment.getRuntimeConfiguration()  // The runtime we shall use
    );

    final String result = Launch.run(clientConfiguration);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains(MESSAGE));
  }

  /**
   * @return the Configuration for Launch for this test.
   */
  private static Configuration getLaunchConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Launch.NumEval.class, "" + (LocalTestEnvironment.NUMBER_OF_THREADS - 1))
        .bindNamedParameter(Launch.NumRuns.class, "2")
        .bindNamedParameter(Launch.Command.class, "echo " + MESSAGE)
        .build();
  }
}
