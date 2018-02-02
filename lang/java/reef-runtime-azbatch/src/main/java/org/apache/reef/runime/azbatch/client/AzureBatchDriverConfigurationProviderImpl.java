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
package org.apache.reef.runime.azbatch.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.driver.AzureBatchDriverConfiguration;
import org.apache.reef.runime.azbatch.driver.RuntimeIdentifier;
import org.apache.reef.runime.azbatch.parameters.*;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.net.URI;

/**
 * Configuration provider for the Azure Batch runtime.
 */
@Private
public final class AzureBatchDriverConfigurationProviderImpl implements DriverConfigurationProvider {

  private final double jvmSlack;
  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchPoolId;
  private final String azureStorageAccountName;
  private final String azureStorageAccountKey;
  private final String azureStorageContainerName;

  @Inject
  AzureBatchDriverConfigurationProviderImpl(
      @Parameter(JVMHeapSlack.class) final double jvmSlack,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId,
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageAccountKey.class) final String azureStorageAccountKey,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName) {
    this.jvmSlack = jvmSlack;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchPoolId = azureBatchPoolId;
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageAccountKey = azureStorageAccountKey;
    this.azureStorageContainerName = azureStorageContainerName;
  }

  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(AzureBatchDriverConfiguration.CONF
            .set(AzureBatchDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(AzureBatchDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(AzureBatchDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
            .set(AzureBatchDriverConfiguration.RUNTIME_NAME, RuntimeIdentifier.RUNTIME_NAME)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_URI, this.azureBatchAccountUri)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_NAME, this.azureBatchAccountName)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_KEY, this.azureBatchAccountKey)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_POOL_ID, this.azureBatchPoolId)
            .set(AzureBatchDriverConfiguration.AZURE_STORAGE_ACCOUNT_NAME, this.azureStorageAccountName)
            .set(AzureBatchDriverConfiguration.AZURE_STORAGE_ACCOUNT_KEY, this.azureStorageAccountKey)
            .set(AzureBatchDriverConfiguration.AZURE_STORAGE_CONTAINER_NAME, this.azureStorageContainerName)
            .build(),
        applicationConfiguration);
  }
}
