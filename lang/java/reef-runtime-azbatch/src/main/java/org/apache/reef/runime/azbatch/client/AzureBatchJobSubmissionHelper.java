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

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.*;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link AzureBatchJobSubmissionHelper} for Azure Batch.
 */
public class AzureBatchJobSubmissionHelper implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(AzureBatchJobSubmissionHandler.class.getName());

  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;
  private final String azureBatchPoolId;

  private final String applicationId;

  public AzureBatchJobSubmissionHelper(
      final String azureBatchAccountUri,
      final String azureBatchAccountName,
      final String azureBatchAccountKey,
      final String azureBatchPoolId,
      final String applicationId) {
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
    this.azureBatchPoolId = azureBatchPoolId;
    this.applicationId = applicationId;
  }

  public void submit(final URI jobJarSasUri, final String command) throws BatchErrorException, IOException {

    BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
        this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
    BatchClient client = BatchClient.open(cred);

    PoolInformation poolInfo = new PoolInformation();
    poolInfo.withPoolId(this.azureBatchPoolId);

    ResourceFile jarResourceFile = new ResourceFile()
        .withBlobSource(jobJarSasUri.toString())
        .withFilePath("local.jar");

    JobManagerTask jobManagerTask = new JobManagerTask()
        .withId(applicationId)
        .withResourceFiles(Collections.singletonList(jarResourceFile))
        .withCommandLine(command);

    LOG.log(Level.INFO, "Job Manager (aka driver) task command: " + command);

    JobAddParameter jobAddParameter = new JobAddParameter()
        .withId(applicationId)
        .withJobManagerTask(jobManagerTask)
        .withPoolInfo(poolInfo);

    client.jobOperations().createJob(jobAddParameter);
  }

  @Override
  public void close() {

  }
}
