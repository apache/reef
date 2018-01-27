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
package org.apache.reef.runime.azbatch.util;

import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.*;
import org.apache.reef.runime.azbatch.parameters.AzureStorageAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureStorageAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureStorageContainerName;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.BufferedInputStream;
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
 * Azure Storage utility to upload Driver and Evaluator jars to Storage
 * and generate SAS URIs.
 */
public class AzureStorageUtil {
  private static final Logger LOG = Logger.getLogger(AzureStorageUtil.class.getName());

  private static final int SAS_TOKEN_VALIDITY_MINUTES = 30;
  private static final String AZURE_STORAGE_CONNECTION_STRING_FORMAT =
      "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

  private final CloudStorageAccount storageAccount;
  private final CloudBlobClient blobClient;
  private final CloudBlobContainer container;
  private final String azureStorageContainerName;

  @Inject
  AzureStorageUtil(
      @Parameter(AzureStorageAccountName.class) final String accountName,
      @Parameter(AzureStorageAccountKey.class) final String accountKey,
      @Parameter(AzureStorageContainerName.class) final String azureStorageContainerName)
      throws URISyntaxException, InvalidKeyException, StorageException {

    final String connectionString = getStorageConnectionString(accountName, accountKey);
    this.storageAccount = CloudStorageAccount.parse(connectionString);
    this.blobClient = this.storageAccount.createCloudBlobClient();
    this.azureStorageContainerName = azureStorageContainerName;
    this.container = this.blobClient.getContainerReference(this.azureStorageContainerName);
    this.container.createIfNotExists();

    LOG.log(Level.FINE, "Instantiated AzureStorageUtil connected to azure storage account: {0}", accountName);
  }

  /**
   * Assemble a connection string from account name and key.
   */
  private static String getStorageConnectionString(final String accountName, final String accountKey) {
    return String.format(AZURE_STORAGE_CONNECTION_STRING_FORMAT, accountName, accountKey);
  }

  public URI createFolder(final String folderName) throws IOException {
    try {
      final CloudBlockBlob jobFolderBlob = this.container.getBlockBlobReference(folderName);
      return jobFolderBlob.getStorageUri().getPrimaryUri();
    } catch (final StorageException | URISyntaxException e) {
      throw new IOException("Unable to create job Folder", e);
    }
  }

  public URI uploadFile(final URI folder, final File file) throws IOException {

    LOG.log(Level.INFO, "Uploading [{0}] to [{1}]", new Object[]{file, folder});

    try {
      final CloudBlockBlob blob = this.container.getBlockBlobReference(String.format("%s/%s", folder, file.getName()));

      try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
        blob.upload(in, file.length());
      }

      LOG.log(Level.FINE, "Uploaded to: {0}", blob.getStorageUri().getPrimaryUri());

      final String sas = blob.generateSharedAccessSignature(getSharedAccessBlobPolicy(), null);
      final String uri = blob.getStorageUri().getPrimaryUri().toString();
      return new URI(uri + "?" + sas);

    } catch (final URISyntaxException | StorageException | InvalidKeyException e) {
      throw new IOException(e);
    }
  }

  private SharedAccessBlobPolicy getSharedAccessBlobPolicy() {

    Date now = Calendar.getInstance().getTime();

    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, SAS_TOKEN_VALIDITY_MINUTES);
    Date tokenExpirationDate = calendar.getTime();

    final SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
    policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ));
    policy.setSharedAccessStartTime(now);
    policy.setSharedAccessExpiryTime(tokenExpirationDate);

    return policy;
  }
}
