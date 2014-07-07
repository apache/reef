/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.hdinsight.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.hdinsight.client.yarnrest.FileResource;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountContainerName;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountKey;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountName;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageBaseFolder;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.BlobProperties;
import com.microsoft.windowsazure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.storage.blob.CloudBlobContainer;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to upload the job JAR to Azure block storage.
 */
@ClientSide
@Private
final class AzureUploader {

  private static final Logger LOG = Logger.getLogger(AzureUploader.class.getName());

  private final CloudStorageAccount storageAccount;
  private final CloudBlobClient blobClient;
  private final CloudBlobContainer container;
  private final String azureStorageContainerName;
  private final String baseFolder;
  private String applicationID;
  private String jobFolderName;

  @Inject
  AzureUploader(
      final @Parameter(AzureStorageAccountName.class) String accountName,
      final @Parameter(AzureStorageAccountKey.class) String accountKey,
      final @Parameter(AzureStorageAccountContainerName.class) String azureStorageContainerName,
      final @Parameter(AzureStorageBaseFolder.class) String baseFolder)
      throws URISyntaxException, InvalidKeyException, StorageException {

    this.storageAccount = CloudStorageAccount.parse(getStorageConnectionString(accountName, accountKey));
    this.blobClient = this.storageAccount.createCloudBlobClient();
    this.azureStorageContainerName = azureStorageContainerName;
    this.container = this.blobClient.getContainerReference(azureStorageContainerName);
    this.container.createIfNotExists();
    this.baseFolder = baseFolder;

    LOG.log(Level.FINE, "Instantiated AzureUploader connected to azure storage account: {0}", accountName);
  }

  public String createJobFolder(final String applicationID) throws IOException {
    try {
      this.applicationID = applicationID;
      this.jobFolderName = assembleJobFolderName(applicationID);
      // Make the directory entry for the job
      final CloudBlockBlob jobFolderBlob = this.container.getBlockBlobReference(this.jobFolderName);
      final String jobFolderURL = getFileSystemURL(jobFolderBlob);
      return jobFolderURL;
    } catch (final StorageException | URISyntaxException e) {
      throw new IOException("Unable to create job Folder", e);
    }
  }

  public FileResource uploadFile(final File file) throws IOException {

    final String destination = this.jobFolderName + "/" + file.getName();
    LOG.log(Level.INFO, "Uploading [{0}] to [{1}]", new Object[]{file, destination});

    try {

      final CloudBlockBlob jobJarBlob = this.container.getBlockBlobReference(destination);

      try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
        jobJarBlob.upload(in, file.length());
      }

      if (!jobJarBlob.exists()) {
        // TODO: If I don't do this check, the getLength() call below returns 0. No idea why.
        LOG.log(Level.WARNING, "Blob doesn't exist!");
      }

      LOG.log(Level.FINE, "Uploaded to: {0}",
          jobJarBlob.getStorageUri().getPrimaryUri());

      // Assemble the FileResource
      final BlobProperties blobProperties = jobJarBlob.getProperties();
      return new FileResource()
          .setType(FileResource.TYPE_ARCHIVE)
          .setVisibility(FileResource.VISIBILITY_APPLICATION)
          .setSize(String.valueOf(blobProperties.getLength()))
          .setTimestamp(String.valueOf(blobProperties.getLastModified().getTime()))
          .setUrl(getFileSystemURL(jobJarBlob));

    } catch (final URISyntaxException | StorageException e) {
      throw new IOException(e);
    }
  }

  /**
   * Assemble a connection string from account name and key.
   */
  private static String getStorageConnectionString(final String accountName, final String accountKey) {
    // "DefaultEndpointsProtocol=http;AccountName=[ACCOUNT_NAME];AccountKey=[ACCOUNT_KEY]"
    return "DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey;
  }

  /**
   * @param blob
   * @return a HDFS URL for the blob
   */
  private String getFileSystemURL(final CloudBlockBlob blob) {
    final URI primaryURI = blob.getStorageUri().getPrimaryUri();
    final String path = primaryURI.getPath().replace(this.azureStorageContainerName + "/", "");
    return "wasb://" + this.azureStorageContainerName + "@" + primaryURI.getHost() + path;
  }

  private String assembleJobFolderName(final String applicationID) {
    return this.baseFolder + (this.baseFolder.endsWith("/") ? "" : "/") + applicationID;
  }
}
