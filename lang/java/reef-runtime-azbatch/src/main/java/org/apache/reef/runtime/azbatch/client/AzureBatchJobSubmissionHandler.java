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
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.azbatch.util.batch.AzureBatchHelper;
import org.apache.reef.runtime.azbatch.util.batch.ContainerRegistryProvider;
import org.apache.reef.runtime.azbatch.util.storage.AzureStorageClient;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link JobSubmissionHandler} implementation for Azure Batch runtime.
 */
@Private
public final class AzureBatchJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(AzureBatchJobSubmissionHandler.class.getName());

  /**
   * Maximum number of characters allowed in Azure Batch job name. This limit is imposed by Azure Batch.
   */
  private static final int MAX_CHARS_JOB_NAME = 64;

  private String applicationId;

  private final AzureStorageClient azureStorageClient;
  private final DriverConfigurationProvider driverConfigurationProvider;
  private final JobJarMaker jobJarMaker;
  private final CommandBuilder launchCommandBuilder;
  private final AzureBatchFileNames azureBatchFileNames;
  private final AzureBatchHelper azureBatchHelper;
  private final ContainerRegistryProvider containerRegistryProvider;

  @Inject
  AzureBatchJobSubmissionHandler(
      final AzureStorageClient azureStorageClient,
      final DriverConfigurationProvider driverConfigurationProvider,
      final JobJarMaker jobJarMaker,
      final CommandBuilder launchCommandBuilder,
      final AzureBatchFileNames azureBatchFileNames,
      final AzureBatchHelper azureBatchHelper,
      final ContainerRegistryProvider containerRegistryProvider) {
    this.azureStorageClient = azureStorageClient;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.jobJarMaker = jobJarMaker;
    this.launchCommandBuilder = launchCommandBuilder;
    this.azureBatchHelper = azureBatchHelper;
    this.azureBatchFileNames = azureBatchFileNames;
    this.containerRegistryProvider = containerRegistryProvider;
  }

  /**
   * Returns REEF application id (which corresponds to Azure Batch job id) or null if the application hasn't been
   * submitted yet.
   *
   * @return REEF application id.
   */
  @Override
  public String getApplicationId() {
    return this.applicationId;
  }

  /**
   * Closes the resources.
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.INFO, "Closing " + AzureBatchJobSubmissionHandler.class.getName());
  }

  /**
   * Invoked when JobSubmissionEvent is triggered.
   *
   * @param jobSubmissionEvent triggered job submission event.
   */
  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {
    LOG.log(Level.FINEST, "Submitting job: {0}", jobSubmissionEvent);

    try {
      this.applicationId = createApplicationId(jobSubmissionEvent);
      final String folderName = this.azureBatchFileNames.getStorageJobFolder(this.applicationId);

      LOG.log(Level.FINE, "Creating a job folder on Azure at: {0}.", folderName);
      final URI jobFolderURL = this.azureStorageClient.getJobSubmissionFolderUri(folderName);

      LOG.log(Level.FINE, "Getting a shared access signature for {0}.", folderName);
      final String storageContainerSAS = this.azureStorageClient.createContainerSharedAccessSignature();

      LOG.log(Level.FINE, "Assembling Configuration for the Driver.");
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, this.applicationId,
          jobFolderURL);

      LOG.log(Level.FINE, "Making Job JAR.");
      final File jobSubmissionJarFile =
          this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);

      LOG.log(Level.FINE, "Uploading Job JAR to Azure.");
      final URI jobJarSasUri = this.azureStorageClient.uploadFile(folderName, jobSubmissionJarFile);

      LOG.log(Level.FINE, "Assembling application submission.");
      final String command = this.launchCommandBuilder.buildDriverCommand(jobSubmissionEvent);

      // In the case of containers, the port provider must be read from the driver configuration.
      TcpPortProvider portProvider = Tang.Factory
          .getTang()
          .newInjector(driverConfiguration)
          .getInstance(TcpPortProvider.class);
      this.azureBatchHelper
          .setTcpPortProvider(portProvider)
          .submitJob(getApplicationId(), storageContainerSAS, jobJarSasUri, command);

    } catch (final IOException | InjectionException e) {
      LOG.log(Level.SEVERE, "Error submitting Azure Batch request: {0}", e);
      throw new RuntimeException(e);
    }
  }

  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final String appId,
      final URI jobFolderURL) {
    return this.driverConfigurationProvider.getDriverConfiguration(
        jobFolderURL, jobSubmissionEvent.getRemoteId(), appId, jobSubmissionEvent.getConfiguration());
  }

  private String createApplicationId(final JobSubmissionEvent jobSubmissionEvent) {
    String uuid = UUID.randomUUID().toString();
    String jobIdentifier = jobSubmissionEvent.getIdentifier();
    String jobNameShort = jobIdentifier.length() + 1 + uuid.length() < MAX_CHARS_JOB_NAME ?
        jobIdentifier : jobIdentifier.substring(0, MAX_CHARS_JOB_NAME - uuid.length() - 1);
    return jobNameShort + "-" + uuid;
  }
}
