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

import com.microsoft.azure.batch.protocol.models.BatchErrorException;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.util.AzureStorageUtil;
import org.apache.reef.runime.azbatch.util.CommandBuilder;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link JobSubmissionHandler} for Azure Batch.
 */
@Private
public final class AzureBatchJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(AzureBatchJobSubmissionHandler.class.getName());

  private final String applicationId;

  private final AzureStorageUtil azureStorageUtil;
  private final DriverConfigurationProvider driverConfigurationProvider;
  private final JobJarMaker jobJarMaker;
  private final CommandBuilder launchCommandBuilder;
  private final REEFFileNames reefFileNames;

  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchPoolId;

  @Inject
  AzureBatchJobSubmissionHandler(
      final AzureStorageUtil azureStorageUtil,
      final DriverConfigurationProvider driverConfigurationProvider,
      final JobJarMaker jobJarMaker,
      final CommandBuilder launchCommandBuilder,
      final REEFFileNames reefFileNames,
      @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
      @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
      @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
    this.azureStorageUtil = azureStorageUtil;
    this.driverConfigurationProvider = driverConfigurationProvider;
    this.jobJarMaker = jobJarMaker;
    this.launchCommandBuilder = launchCommandBuilder;

    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchPoolId = azureBatchPoolId;

    this.reefFileNames = reefFileNames;

    this.applicationId = "HelloWorldJob-"
        + this.azureBatchAccountName + "-"
        + (new Date()).toString()
        .replace(' ', '-')
        .replace(':', '-')
        .replace('.', '-');
  }

  @Override
  public String getApplicationId() {
    return this.applicationId;
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.INFO, "Closing " + AzureBatchJobSubmissionHandler.class.getName());
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    LOG.log(Level.FINEST, "Submitting job: {0}", jobSubmissionEvent);

    try (final AzureBatchJobSubmissionHelper helper = new AzureBatchJobSubmissionHelper(
        this.azureBatchAccountUri,
        this.azureBatchAccountName,
        this.azureBatchAccountKey,
        this.azureBatchPoolId,
        getApplicationId())) {

      final String id = jobSubmissionEvent.getIdentifier();
      final String folderName = createJobFolderName(id);

      LOG.log(Level.FINE, "Creating a job folder on Azure at: {0}.", folderName);
      URI jobFolderURL = this.azureStorageUtil.createFolder(folderName);

      LOG.log(Level.FINE, "Assembling Configuration for the Driver.");
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, id, jobFolderURL);

      LOG.log(Level.FINE, "Making Job JAR.");
      final File jobSubmissionJarFile =
          this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);

      LOG.log(Level.FINE, "Uploading Job JAR to Azure.");
      final URI jobJarSasUri = this.azureStorageUtil.uploadFile(jobFolderURL, jobSubmissionJarFile);

      LOG.log(Level.FINE, "Assembling application submission.");
      final String command = this.launchCommandBuilder.build(jobSubmissionEvent);

      helper.submit(jobJarSasUri, command);

    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Error submitting Azure Batch request", ex);
      throw new RuntimeException(ex);
    } catch (final BatchErrorException ex) {
      LOG.log(Level.SEVERE, "An error occurred while calling Azure Batch", ex);
      throw ex;
    }
  }

  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final String appId,
      final URI jobFolderURL) {
    return this.driverConfigurationProvider.getDriverConfiguration(
        jobFolderURL, jobSubmissionEvent.getRemoteId(), appId, jobSubmissionEvent.getConfiguration());
  }

  private String createJobFolderName(final String jobApplicationID) {
    return this.reefFileNames.getAzbatchJobFolderPath() + jobApplicationID;
  }
}
