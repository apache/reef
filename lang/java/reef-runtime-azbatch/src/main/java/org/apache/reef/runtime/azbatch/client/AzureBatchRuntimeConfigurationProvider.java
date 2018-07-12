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

import org.apache.reef.annotations.audience.Public;
import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.azbatch.util.batch.ContainerRegistryProvider;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.parameters.TcpPortSet;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

/**
 * Class that provides the runtime configuration for Azure Batch.
 */
@Public
public final class AzureBatchRuntimeConfigurationProvider {
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchAccountUri;
  private final String azureBatchPoolId;
  private final String azureStorageAccountName;
  private final String azureStorageAccountKey;
  private final String azureStorageContainerName;
  private final ContainerRegistryProvider containerRegistryProvider;
  private final Boolean isWindows;
  private final Set<String> tcpPortSet;

  /**
   * Private constructor.
   */
  @Inject
  private AzureBatchRuntimeConfigurationProvider(
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId,
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageAccountKey.class) final String azureStorageAccountKey,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName,
      @Parameter(IsWindows.class) final Boolean isWindows,
      @Parameter(TcpPortSet.class) final Set<Integer> tcpPortSet,
      final ContainerRegistryProvider containerRegistryProvider) {
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchPoolId = azureBatchPoolId;
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageAccountKey = azureStorageAccountKey;
    this.azureStorageContainerName = azureStorageContainerName;
    this.containerRegistryProvider = containerRegistryProvider;
    this.isWindows = isWindows;

    // Binding a parameter to a set is only allowed for strings, so we cast to strings.
    this.tcpPortSet = new HashSet(tcpPortSet.size());
    for (int port: tcpPortSet) {
      this.tcpPortSet.add(Integer.toString(port));
    }
  }

  public Configuration getAzureBatchRuntimeConfiguration() {
    return AzureBatchRuntimeConfigurationCreator
        .getOrCreateAzureBatchRuntimeConfiguration(this.isWindows)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME, this.azureBatchAccountName)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY, this.azureBatchAccountKey)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI, this.azureBatchAccountUri)
        .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID, this.azureBatchPoolId)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_NAME, this.azureStorageAccountName)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_ACCOUNT_KEY, this.azureStorageAccountKey)
        .set(AzureBatchRuntimeConfiguration.AZURE_STORAGE_CONTAINER_NAME, this.azureStorageContainerName)
        .set(AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_SERVER,
            this.containerRegistryProvider.getContainerRegistryServer())
        .set(AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_USERNAME,
            this.containerRegistryProvider.getContainerRegistryUsername())
        .set(AzureBatchRuntimeConfiguration.CONTAINER_REGISTRY_PASSWORD,
            this.containerRegistryProvider.getContainerRegistryPassword())
        .set(AzureBatchRuntimeConfiguration.CONTAINER_IMAGE_NAME,
            this.containerRegistryProvider.getContainerImageName())
        .setMultiple(AzureBatchRuntimeConfiguration.TCP_PORT_SET, this.tcpPortSet)
        .build();
  }
}
