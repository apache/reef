package com.microsoft.reef.examples.hdinsightcli;

import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountContainerName;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountKey;
import com.microsoft.reef.runtime.hdinsight.parameters.AzureStorageAccountName;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by marku_000 on 2014-07-03.
 */
public final class LogFetcher {
  private final CloudBlobContainer container;
  private static final String LOG_FOLDER_PREFIX = "app-logs/gopher/logs/";
  private static final Logger LOG = Logger.getLogger(LogFetcher.class.getName());


  @Inject
  LogFetcher(final @Parameter(AzureStorageAccountName.class) String accountName,
             final @Parameter(AzureStorageAccountKey.class) String accountKey,
             final @Parameter(AzureStorageAccountContainerName.class) String azureStorageContainerName)
      throws URISyntaxException, InvalidKeyException, StorageException {
    this.container = getContainer(accountName, accountKey, azureStorageContainerName);
  }

  public void fetch(final String applicationId, final Writer outputWriter) throws IOException {
    try {
      final File localFolder = downloadToTempFolder(applicationId);

      final Configuration hadoopConfiguration = new Configuration();
      final FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
      final TFileParser tFileParser = new TFileParser(hadoopConfiguration, fileSystem);

      final Path localFolderPath = new Path(localFolder.getAbsolutePath());
      for (final FileStatus fileStatus : fileSystem.listStatus(localFolderPath)) {
        tFileParser.parseOneFile(fileStatus.getPath(), outputWriter);
      }


    } catch (final Exception e) {
      throw new IOException(e);
    }

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
  private File downloadToTempFolder(final String applicationId) throws URISyntaxException, StorageException, IOException {
    final File outputFolder = Files.createTempDirectory("reeflogs-" + applicationId).toFile();
    outputFolder.mkdirs();
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
    LOG.log(Level.INFO, "Downloaded logs to: {0}", outputFolder.getAbsolutePath());
    return outputFolder;
  }


  private static CloudBlobContainer getContainer(final String accountName,
                                                 final String accountKey,
                                                 final String containerName)
      throws URISyntaxException, InvalidKeyException, StorageException {
    final CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(getStorageConnectionString(accountName, accountKey));
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


}
