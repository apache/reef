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
package org.apache.reef.runtime.azbatch.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.azbatch.util.command.LinuxCommandBuilder;
import org.apache.reef.runtime.azbatch.util.command.WindowsCommandBuilder;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.wake.remote.ports.parameters.TcpPortSet;

/**
 * Class that builds the ConfigurationModule for Azure Batch runtime.
 */
@Private
public final class AzureBatchRuntimeConfigurationCreator {

  /**
   * The ConfigurationModule for Azure Batch.
   */
  private static ConfigurationModule conf;

  /**
   * Get or create a {@link ConfigurationModule} for the Azure Batch runtime.
   *
   * @param isWindows true if Azure Batch pool nodes run Windows, false otherwise.
   * @return the configuration module object.
   */
  public static ConfigurationModule getOrCreateAzureBatchRuntimeConfiguration(
      final boolean isWindows) {

    if (AzureBatchRuntimeConfigurationCreator.conf == null) {
      ConfigurationModuleBuilder builder = AzureBatchRuntimeConfigurationStatic.CONF;

      if (isWindows) {
        builder = builder.bindImplementation(CommandBuilder.class, WindowsCommandBuilder.class);
      } else {
        builder = builder.bindImplementation(CommandBuilder.class, LinuxCommandBuilder.class);
      }

      AzureBatchRuntimeConfigurationCreator.conf = new AzureBatchRuntimeConfiguration()
          .merge(builder.build())
          .bindNamedParameter(AzureBatchAccountName.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME)
          .bindNamedParameter(AzureBatchAccountUri.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI)
          .bindNamedParameter(AzureBatchAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY)
          .bindNamedParameter(AzureBatchPoolId.class, AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID)
          .bindNamedParameter(AzureStorageAccountName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME)
          .bindNamedParameter(AzureStorageAccountKey.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY)
          .bindNamedParameter(ContainerRegistryServer.class, AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_SERVER)
          .bindNamedParameter(
              ContainerRegistryUsername.class, AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_USERNAME)
          .bindNamedParameter(
              ContainerRegistryPassword.class, AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_PASSWORD)
          .bindNamedParameter(ContainerImageName.class, AzureBatchRuntimeConfiguration.CONTAINER_IMAGE_NAME)
          .bindNamedParameter(
              AzureStorageContainerName.class, AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME)
          .bindSetEntry(TcpPortSet.class, AzureBatchRuntimeConfiguration.TCP_PORT_SET)
          .build();
    }

    return AzureBatchRuntimeConfigurationCreator.conf;
  }

  /*
   * Private constructor since this is a utility class.
   */
  private AzureBatchRuntimeConfigurationCreator() {
  }
}
