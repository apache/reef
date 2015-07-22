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
package org.apache.reef.tests.configurationproviders;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.parameters.DriverConfigurationProviders;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the ConfigurationProvider mechanism.
 */
public final class ConfigurationProviderTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  /**
   * Uses the TestEnvironment to get the base config and adds the DriverConfigurationProvider.
   *
   * @return the runtime configuration.
   */
  private Configuration getRuntimeConfiguration() {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(testEnvironment.getRuntimeConfiguration())
        .bindSetEntry(DriverConfigurationProviders.class, TestDriverConfigurationProvider.class)
        .build();
  }

  /**
   * Assembles the Driver configuration.
   *
   * @return the Driver configuration.
   */
  private Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "ConfigurationProviderTest")
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            EnvironmentUtils.getClassLocation(ConfigurationProviderTestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, ConfigurationProviderTestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
            ConfigurationProviderTestDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  /**
   * Tests whether parameters get propagated correctly when set via the ConfigurationProvider mechanism.
   *
   * @throws InjectionException
   */
  @Test
  public void testConfigurationProviders() throws InjectionException {
    final LauncherStatus status = DriverLauncher.getLauncher(getRuntimeConfiguration())
        .run(getDriverConfiguration(), testEnvironment.getTestTimeout());
    Assert.assertTrue("ConfigurationProviderTest completed with status: " + status, status.isSuccess());
  }
}
