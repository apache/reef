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

import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.driver.AzureBatchDriverConfiguration;
import org.apache.reef.runtime.azbatch.driver.RuntimeIdentifier;
import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.wake.remote.address.ContainerBasedLocalAddressProvider;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.ListTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortListString;

import javax.inject.Inject;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
  private final String containerRegistryServer;
  private final String containerRegistryUsername;
  private final String containerRegistryPassword;
  private final String containerImageName;
  private final CommandBuilder commandBuilder;
  private final String tcpPortListString;

  @Inject
  private AzureBatchDriverConfigurationProviderImpl(
      @Parameter(JVMHeapSlack.class) final double jvmSlack,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId,
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName,
      @Parameter(ContainerRegistryServer.class) final String containerRegistryServer,
      @Parameter(ContainerRegistryUsername.class) final String containerRegistryUsername,
      @Parameter(ContainerRegistryPassword.class) final String containerRegistryPassword,
      @Parameter(ContainerImageName.class) final String containerImageName,
      @Parameter(TcpPortListString.class) final String tcpPortListString,
      final CommandBuilder commandBuilder) {
    this.jvmSlack = jvmSlack;
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchPoolId = azureBatchPoolId;
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageContainerName = azureStorageContainerName;
    this.containerRegistryServer = containerRegistryServer;
    this.containerRegistryUsername = containerRegistryUsername;

    // TODO: Secure the password by encrypting it and using
    // Azure Batch certificates to decrypt on the driver side.
    this.containerRegistryPassword = containerRegistryPassword;
    this.containerImageName = containerImageName;
    this.commandBuilder = commandBuilder;
    this.tcpPortListString = tcpPortListString;
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


    ConfigurationModuleBuilder driverConfigurationBuilder = AzureBatchDriverConfiguration.CONF.getBuilder()
        .bindImplementation(CommandBuilder.class, this.commandBuilder.getClass());

    // If using docker containers, then use a different set of bindings
    if (!StringUtils.isEmpty(this.containerRegistryServer)) {
      driverConfigurationBuilder = driverConfigurationBuilder
          .bindImplementation(LocalAddressProvider.class, ContainerBasedLocalAddressProvider.class)
          .bindImplementation(TcpPortProvider.class, ListTcpPortProvider.class);
    }

    Configuration driverConfiguration = driverConfigurationBuilder.build()
        .set(AzureBatchDriverConfiguration.JOB_IDENTIFIER, jobId)
        .set(AzureBatchDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, clientRemoteId)
        .set(AzureBatchDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
        .set(AzureBatchDriverConfiguration.RUNTIME_NAME, RuntimeIdentifier.RUNTIME_NAME)
        .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_URI, this.azureBatchAccountUri)
        .set(AzureBatchDriverConfiguration.AZURE_BATCH_ACCOUNT_NAME, this.azureBatchAccountName)
        .set(AzureBatchDriverConfiguration.AZURE_BATCH_POOL_ID, this.azureBatchPoolId)
        .set(AzureBatchDriverConfiguration.AZURE_STORAGE_ACCOUNT_NAME, this.azureStorageAccountName)
        .set(AzureBatchDriverConfiguration.AZURE_STORAGE_CONTAINER_NAME, this.azureStorageContainerName)
        .set(AzureBatchDriverConfiguration.CONTAINER_REGISTRY_SERVER, this.containerRegistryServer)
        .set(AzureBatchDriverConfiguration.CONTAINER_REGISTRY_USERNAME, this.containerRegistryUsername)
        .set(AzureBatchDriverConfiguration.CONTAINER_REGISTRY_PASSWORD, this.containerRegistryPassword)
        .set(AzureBatchDriverConfiguration.CONTAINER_IMAGE_NAME, this.containerImageName)
        .set(AzureBatchDriverConfiguration.TCP_PORT_LIST_STRING, this.tcpPortListString)
        .build();
    return Configurations.merge(driverConfiguration, applicationConfiguration);
  }
}
