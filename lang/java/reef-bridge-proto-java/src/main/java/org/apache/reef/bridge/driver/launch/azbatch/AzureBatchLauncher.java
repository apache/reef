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
package org.apache.reef.bridge.driver.launch.azbatch;

import org.apache.reef.bridge.driver.launch.IDriverLauncher;
import org.apache.reef.bridge.driver.service.IDriverServiceConfigurationProvider;
import org.apache.reef.bridge.proto.ClientProtocol;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfiguration;
import org.apache.reef.runtime.azbatch.client.AzureBatchRuntimeConfigurationCreator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a bootstrap launcher for Azure Batch for submission from C#. It allows for Java Driver
 * configuration generation directly on the Driver without need of Java dependency if REST
 * submission is used.
 */
public final class AzureBatchLauncher implements IDriverLauncher {

  private static final Logger LOG = Logger.getLogger(AzureBatchLauncher.class.getName());
  private static final Tang TANG = Tang.Factory.getTang();

  private final IDriverServiceConfigurationProvider driverServiceConfigurationProvider;

  @Inject
  private AzureBatchLauncher(final IDriverServiceConfigurationProvider driverServiceConfigurationProvider) {
    this.driverServiceConfigurationProvider = driverServiceConfigurationProvider;
  }

  public void launch(final ClientProtocol.DriverClientConfiguration driverClientConfiguration) {
    try (final REEF reef = TANG.newInjector(
        generateConfigurationFromJobSubmissionParameters(driverClientConfiguration)).getInstance(REEF.class)) {
      LOG.log(Level.INFO, "Submitting job");
      reef.submit(driverServiceConfigurationProvider.getDriverServiceConfiguration(driverClientConfiguration));
    } catch (InjectionException e) {
      fatal("unable to launch", e);
    }
    LOG.log(Level.INFO, "Exiting BootstrapLauncher.main()");

    System.exit(0); // TODO[REEF-1715]: Should be able to exit cleanly at the end of main()
  }

  private static Configuration generateConfigurationFromJobSubmissionParameters(
      final ClientProtocol.DriverClientConfiguration driverClientConfiguration) {
    return AzureBatchRuntimeConfigurationCreator.getOrCreateAzureBatchRuntimeConfiguration(
        driverClientConfiguration.getOperatingSystem() ==
            ClientProtocol.DriverClientConfiguration.OS.WINDOWS)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME,
            driverClientConfiguration.getAzbatchRuntime().getAzureBatchAccountName())
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY,
            driverClientConfiguration.getAzbatchRuntime().getAzureBatchAccountKey())
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI,
            driverClientConfiguration.getAzbatchRuntime().getAzureBatchAccountUri())
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID,
            driverClientConfiguration.getAzbatchRuntime().getAzureBatchPoolId())
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME,
            driverClientConfiguration.getAzbatchRuntime().getAzureStorageAccountName())
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY,
            driverClientConfiguration.getAzbatchRuntime().getAzureStorageAccountKey())
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME,
            driverClientConfiguration.getAzbatchRuntime().getAzureStorageContainerName())
        .build();
  }

  private static RuntimeException fatal(final String msg, final Throwable t) {
    LOG.log(Level.SEVERE, msg, t);
    return new RuntimeException(msg, t);
  }
}
