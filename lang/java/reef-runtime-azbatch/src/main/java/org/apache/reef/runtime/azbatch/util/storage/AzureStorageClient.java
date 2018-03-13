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
package org.apache.reef.runtime.azbatch.util.storage;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.*;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageBlobSASTokenValidityHours;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageContainerName;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure storage utility to upload Driver and Evaluator jars to blob storage
 * and generate SAS URIs.
 */
public class AzureStorageClient {
  private static final Logger LOG = Logger.getLogger(AzureStorageClient.class.getName());

  private final ICloudBlobClientProvider cloudBlobClientProvider;

  private final String azureStorageContainerName;
  private final int blobSASTokenValidityHours;

  @Inject
  AzureStorageClient(
      final ICloudBlobClientProvider cloudBlobClientProvider,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName,
      @Parameter(AzureStorageBlobSASTokenValidityHours.class) final int blobSASTokenValidityHours) {
    this.cloudBlobClientProvider = cloudBlobClientProvider;
    this.azureStorageContainerName = azureStorageContainerName;
    this.blobSASTokenValidityHours = blobSASTokenValidityHours;
  }

  public URI getJobSubmissionFolderUri(final String jobFolder) throws IOException {
    final CloudBlobClient cloudBlobClient = this.cloudBlobClientProvider.getCloudBlobClient();
    try {
      final CloudBlobContainer container = cloudBlobClient.getContainerReference(this.azureStorageContainerName);
      return container.getDirectoryReference(jobFolder).getUri();
    } catch (URISyntaxException | StorageException e) {
      throw new IOException("Failed to get the job submission folder URI", e);
    }
  }


  public String createContainerSharedAccessSignature() throws IOException {
    try {
      CloudBlobClient cloudBlobClient = this.cloudBlobClientProvider.getCloudBlobClient();
      CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(this.azureStorageContainerName);
      cloudBlobContainer.createIfNotExists();

      return cloudBlobContainer.generateSharedAccessSignature(getSharedAccessContainerPolicy(), null);

    } catch (StorageException | URISyntaxException | InvalidKeyException e) {
      throw new IOException("Failed to generate a shared access signature for storage container.", e);
    }
  }


  /**
   * Upload a file to the storage account.
   *
   * @param jobFolder the path to the destination folder within storage container.
   * @param file the source file.
   * @return the SAS URI to the uploaded file.
   * @throws IOException
   */
  public URI uploadFile(final String jobFolder, final File file) throws IOException {

    LOG.log(Level.INFO, "Uploading [{0}] to [{1}]", new Object[]{file, jobFolder});

    try {
      final CloudBlobClient cloudBlobClient = this.cloudBlobClientProvider.getCloudBlobClient();
      final CloudBlobContainer container = cloudBlobClient.getContainerReference(this.azureStorageContainerName);

      final String destination = String.format("%s/%s", jobFolder, file.getName());
      final CloudBlockBlob blob = container.getBlockBlobReference(destination);

      try (FileInputStream fis = new FileInputStream(file)) {
        blob.upload(fis, file.length());
      }

      LOG.log(Level.FINE, "Uploaded to: {0}", blob.getStorageUri().getPrimaryUri());
      return this.cloudBlobClientProvider.generateSharedAccessSignature(blob, getSharedAccessBlobPolicy());

    } catch (final URISyntaxException | StorageException e) {
      throw new IOException(e);
    }
  }

  private SharedAccessBlobPolicy getSharedAccessBlobPolicy() {
    return getSharedAccessBlobPolicy(EnumSet.of(SharedAccessBlobPermissions.READ));
  }

  private SharedAccessBlobPolicy getSharedAccessContainerPolicy() {
    return getSharedAccessBlobPolicy(EnumSet.of(SharedAccessBlobPermissions.READ, SharedAccessBlobPermissions.WRITE));
  }

  private SharedAccessBlobPolicy getSharedAccessBlobPolicy(
      final EnumSet<SharedAccessBlobPermissions> permissions) {

    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.HOUR, this.blobSASTokenValidityHours);
    Date tokenExpirationDate = calendar.getTime();

    final SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
    policy.setPermissions(permissions);
    policy.setSharedAccessStartTime(Calendar.getInstance().getTime());
    policy.setSharedAccessExpiryTime(tokenExpirationDate);

    return policy;
  }
}
