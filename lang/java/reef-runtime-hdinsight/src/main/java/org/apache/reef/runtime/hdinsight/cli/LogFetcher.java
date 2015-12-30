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
package org.apache.reef.runtime.hdinsight.cli;

import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountContainerName;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountKey;
import org.apache.reef.runtime.hdinsight.parameters.AzureStorageAccountName;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to fetch logs from an HDInsight cluster.
 */
final class LogFetcher {
  private static final String LOG_FOLDER_PREFIX = "app-logs/gopher/logs/";
  private static final Logger LOG = Logger.getLogger(LogFetcher.class.getName());
  private final CloudBlobContainer container;
  private final FileSystem fileSystem;
  private final Configuration hadoopConfiguration;
  private final TFileParser tFileParser;


  @Inject
  LogFetcher(@Parameter(AzureStorageAccountName.class) final String accountName,
             @Parameter(AzureStorageAccountKey.class) final String accountKey,
             @Parameter(AzureStorageAccountContainerName.class) final String azureStorageContainerName)
      throws URISyntaxException, InvalidKeyException, StorageException, IOException {
    this.container = getContainer(accountName, accountKey, azureStorageContainerName);
    this.hadoopConfiguration = new Configuration();
    this.fileSystem = FileSystem.get(hadoopConfiguration);
    this.tFileParser = new TFileParser(hadoopConfiguration, fileSystem);
  }

  private static CloudBlobContainer getContainer(final String accountName,
                                                 final String accountKey,
                                                 final String containerName)
      throws URISyntaxException, InvalidKeyException, StorageException {
    final CloudStorageAccount cloudStorageAccount =
        CloudStorageAccount.parse(getStorageConnectionString(accountName, accountKey));
    final CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
    return blobClient.getContainerReference(containerName);
  }

  /**
   * Assemble a connection string from account name and key.
   */
  private static String getStorageConnectionString(final String accountName, final String accountKey) {
    // "DefaultEndpointsProtocol=http;AccountName=[ACCOUNT_NAME];AccountKey=[ACCOUNT_KEY]"
    return "DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey;
  }

  void fetch(final String applicationId, final Writer outputWriter) throws IOException {
    try {
      for (final FileStatus fileStatus : downloadLogs(applicationId)) {
        tFileParser.parseOneFile(fileStatus.getPath(), outputWriter);
      }
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  void fetch(final String applicationId, final File folder) throws IOException {
    try {
      for (final FileStatus fileStatus : downloadLogs(applicationId)) {
        tFileParser.parseOneFile(fileStatus.getPath(), folder);
      }
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  private FileStatus[] downloadLogs(final String applicationId)
      throws StorageException, IOException, URISyntaxException {
    final File localFolder = downloadToTempFolder(applicationId);
    final Path localFolderPath = new Path(localFolder.getAbsolutePath());
    return this.fileSystem.listStatus(localFolderPath);
  }

  /**
   * Downloads the logs to a local temp folder.
   *
   * @param applicationId
   * @return
   * @throws URISyntaxException
   * @throws StorageException
   * @throws IOException
   */
  private File downloadToTempFolder(final String applicationId)
      throws URISyntaxException, StorageException, IOException {
    final File outputFolder = Files.createTempDirectory("reeflogs-" + applicationId).toFile();
    if (!outputFolder.exists() && !outputFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", outputFolder.getAbsolutePath());
    }
    final CloudBlobDirectory logFolder = this.container.getDirectoryReference(LOG_FOLDER_PREFIX + applicationId + "/");
    int fileCounter = 0;
    for (final ListBlobItem blobItem : logFolder.listBlobs()) {
      if (blobItem instanceof CloudBlob) {
        try (final OutputStream outputStream = new FileOutputStream(new File(outputFolder, "File-" + fileCounter))) {
          ((CloudBlob) blobItem).download(outputStream);
          ++fileCounter;
        }
      }
    }
    LOG.log(Level.FINE, "Downloaded logs to: {0}", outputFolder.getAbsolutePath());
    return outputFolder;
  }


}
