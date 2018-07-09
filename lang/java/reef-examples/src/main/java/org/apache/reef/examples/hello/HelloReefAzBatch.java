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
package org.apache.reef.examples.hello;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfiguration;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfigurationProvider;
import org.apache.reef.runtime.azbatch.parameters.ContainerRegistryServer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.remote.ports.SetTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main() for running hello REEF in Azure Batch.
 */
public final class HelloReefAzBatch {

  private static final Logger LOG = Logger.getLogger(HelloReefAzBatch.class.getName());

  /**
   * Builds the runtime configuration for Azure Batch.
   *
   * @return the configuration of the runtime.
   * @throws IOException
   */
  private static Configuration getEnvironmentConfiguration() throws IOException {
    return AzureBatchRuntimeConfiguration.fromEnvironment();
  }

  /**
   * Builds and returns driver configuration for HelloREEF driver.
   *
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration(boolean includeContainerConfiguration) {
    Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();

    if (!includeContainerConfiguration) {
      return driverConfiguration;
    } else {
      final Configuration containerConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(TcpPortProvider.class, SetTcpPortProvider.class)
          .build();
      return Configurations.merge(containerConfiguration, driverConfiguration);
    }
  }

  /**
   * Start the Hello REEF job with the Azure Batch runtime.
   *
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   * @throws IOException
   */
  public static void main(final String[] args) throws InjectionException, IOException {

    final Configuration partialConfiguration = getEnvironmentConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(partialConfiguration);
    final String containerRegistryServer = injector.getNamedInstance(ContainerRegistryServer.class);
    final AzureBatchRuntimeConfigurationProvider runtimeConfigurationProvider =
        injector.getInstance(AzureBatchRuntimeConfigurationProvider.class);
    final Configuration driverConfiguration = getDriverConfiguration(!StringUtils.isEmpty(containerRegistryServer));

    try (final REEF reef = Tang.Factory.getTang().newInjector(
        runtimeConfigurationProvider.getAzureBatchRuntimeConfiguration()).getInstance(REEF.class)) {
      reef.submit(driverConfiguration);
    }
    LOG.log(Level.INFO, "Job Submitted");
  }

  /**
   * Private constructor.
   */
  private HelloReefAzBatch() {
  }
}
