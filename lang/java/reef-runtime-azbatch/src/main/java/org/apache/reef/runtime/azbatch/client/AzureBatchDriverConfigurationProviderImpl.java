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
import org.apache.reef.runtime.azbatch.driver.AzureBatchDriverConfiguration;
import org.apache.reef.runtime.azbatch.driver.RuntimeIdentifier;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageAccountName;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageContainerName;
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
  private final String azureBatchPoolId;
  private final String azureStorageAccountName;
  private final String azureStorageContainerName;
  private final CommandBuilder commandBuilder;

  @Inject
  AzureBatchDriverConfigurationProviderImpl(
      @Parameter(JVMHeapSlack.class) final double jvmSlack,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId,
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName,
      final CommandBuilder commandBuilder) {
    this.jvmSlack = jvmSlack;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchPoolId = azureBatchPoolId;
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageContainerName = azureStorageContainerName;
    this.commandBuilder = commandBuilder;
  }

  /**
   * Assembles the Driver configuration.
   *
   * @param jobFolder the job folder.
   * @param clientRemoteId the client remote id.
   * @param jobId the job id.
   * @param applicationConfiguration the application configuration.
   * @return the Driver configuration.
   */
  @Override
  public Configuration getDriverConfiguration(final URI jobFolder,
                                              final String clientRemoteId,
                                              final String jobId,
                                              final Configuration applicationConfiguration) {
    return Configurations.merge(
        AzureBatchDriverConfiguration.CONF.getBuilder()
            .bindImplementation(CommandBuilder.class, this.commandBuilder.getClass()).build()
            .set(AzureBatchDriverConfiguration.JOB_IDENTIFIER, jobId)
            .set(AzureBatchDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
            .set(AzureBatchDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
            .set(AzureBatchDriverConfiguration.RUNTIME_NAME, RuntimeIdentifier.RUNTIME_NAME)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_URI, this.azureBatchAccountUri)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_NAME, this.azureBatchAccountName)
            .set(AzureBatchDriverConfiguration.AZURE_BATCH_POOL_ID, this.azureBatchPoolId)
            .set(AzureBatchDriverConfiguration.AZURE_STORAGE_ACCOUNT_NAME, this.azureStorageAccountName)
            .set(AzureBatchDriverConfiguration.AZURE_STORAGE_CONTAINER_NAME, this.azureStorageContainerName)
            .build(),
        applicationConfiguration);
  }
}
