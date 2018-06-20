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
package org.apache.reef.runtime.azbatch.evaluator;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.CloudBlob;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.EvaluatorShimProtocol;
import org.apache.reef.runtime.azbatch.parameters.ContainerIdentifier;
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * The evaluator shim acts as a wrapper process around the Evaluator. Azure Batch starts this process on the evaluator
 * node at the time that the resource is allocated. Once started, the evaluator shim process will send a status
 * message back to the the Driver which triggers a
 * {@link org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent} on the Driver side.
 * The evaluator shim will then wait for a command from the Driver to start the Evaluator process.
 * Upon receiving the command, the shim will launch the evaluator process and wait for it to exit. After receiving
 * a terminate command, the evaluator shim will exit thus releasing the resource and completing the Azure Batch task.
 */
@Private
@EvaluatorSide
public final class EvaluatorShim
    implements EventHandler<RemoteMessage<EvaluatorShimProtocol.EvaluatorShimControlProto>> {
  private static final Logger LOG = Logger.getLogger(EvaluatorShim.class.getName());

  private final RemoteManager remoteManager;
  private final REEFFileNames reefFileNames;
  private final AzureBatchFileNames azureBatchFileNames;
  private final ConfigurationSerializer configurationSerializer;

  private final String driverRemoteId;
  private final String containerId;

  private final EventHandler<EvaluatorShimProtocol.EvaluatorShimStatusProto> evaluatorShimStatusChannel;
  private final AutoCloseable evaluatorShimCommandChannel;

  private final ExecutorService threadPool;

  private Process evaluatorProcess;
  private Integer evaluatorProcessExitValue;

  @Inject
  EvaluatorShim(final REEFFileNames reefFileNames,
                final AzureBatchFileNames azureBatchFileNames,
                final ConfigurationSerializer configurationSerializer,
                final RemoteManager remoteManager,
                @Parameter(DriverRemoteIdentifier.class) final String driverRemoteId,
                @Parameter(ContainerIdentifier.class) final String containerId) {
    this.reefFileNames = reefFileNames;
    this.azureBatchFileNames = azureBatchFileNames;
    this.configurationSerializer = configurationSerializer;

    this.driverRemoteId = driverRemoteId;
    this.containerId = containerId;

    this.remoteManager = remoteManager;
    this.evaluatorShimStatusChannel = this.remoteManager.getHandler(this.driverRemoteId,
        EvaluatorShimProtocol.EvaluatorShimStatusProto.class);

    this.evaluatorShimCommandChannel = this.remoteManager
        .registerHandler(EvaluatorShimProtocol.EvaluatorShimControlProto.class, this);

    this.threadPool = Executors.newCachedThreadPool();
  }

  /**
   * Starts the {@link EvaluatorShim}.
   */
  public void run() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.run().");
    this.onStart();
  }

  /**
   * Stops the {@link EvaluatorShim}.
   */
  public void stop() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.stop().");
    this.onStop();
  }

  /**
   * This method is invoked by the Remote Manager when a command message from the Driver is received.
   *
   * @param remoteMessage the message sent to the evaluator shim by the Driver.
   */
  @Override
  public void onNext(final RemoteMessage<EvaluatorShimProtocol.EvaluatorShimControlProto> remoteMessage) {
    final EvaluatorShimProtocol.EvaluatorShimCommand command = remoteMessage.getMessage().getCommand();
    switch (command) {
    case LAUNCH_EVALUATOR:
      LOG.log(Level.INFO, "Received a command to launch the Evaluator.");
      this.threadPool.submit(new Runnable() {
        @Override
        public void run() {
          EvaluatorShim.this.onEvaluatorLaunch(remoteMessage.getMessage().getEvaluatorLaunchCommand(),
              remoteMessage.getMessage().getEvaluatorConfigString(),
              remoteMessage.getMessage().getEvaluatorFileResourcesUrl());
        }
      });
      break;

    case TERMINATE:
      LOG.log(Level.INFO, "Received a command to terminate the EvaluatorShim.");
      this.threadPool.submit(new Runnable() {
        @Override
        public void run() {
          EvaluatorShim.this.onStop();
        }
      });
      break;

    default:
      LOG.log(Level.WARNING, "An unknown command was received by the EvaluatorShim: {0}.", command);
      throw new IllegalArgumentException("An unknown command was received by the EvaluatorShim.");
    }
  }

  private void onStart() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.onStart().");

    LOG.log(Level.INFO, "Reporting back to the driver with Shim Status = {0}",
        EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE);
    this.evaluatorShimStatusChannel.onNext(
        EvaluatorShimProtocol.EvaluatorShimStatusProto
            .newBuilder()
            .setRemoteIdentifier(this.remoteManager.getMyIdentifier())
            .setContainerId(this.containerId)
            .setStatus(EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE)
            .build());

    LOG.log(Level.FINEST, "Exiting EvaluatorShim.onStart().");
  }

  private void onStop() {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.onStop().");

    try {
      LOG.log(Level.INFO, "Closing EvaluatorShim Control channel.");
      this.evaluatorShimCommandChannel.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "An unexpected exception occurred while attempting to close the EvaluatorShim " +
          "control channel.");
      throw new RuntimeException(e);
    }

    try {
      LOG.log(Level.INFO, "Closing the Remote Manager.");
      this.remoteManager.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to close the RemoteManager with the following exception: {0}.", e);
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Shutting down the thread pool.");
    this.threadPool.shutdown();

    LOG.log(Level.FINEST, "Exiting EvaluatorShim.onStop().");
  }

  private void onEvaluatorLaunch(final String launchCommand, final String evaluatorConfigString,
                                 final String fileResourcesUrl) {
    LOG.log(Level.FINEST, "Entering EvaluatorShim.onEvaluatorLaunch().");

    if (StringUtils.isNotBlank(fileResourcesUrl)) {
      LOG.log(Level.FINER, "Downloading evaluator resource file archive from {0}.", fileResourcesUrl);
      try {
        File tmpFile = downloadFile(fileResourcesUrl);
        extractFiles(tmpFile);
      } catch (StorageException | IOException e) {
        LOG.log(Level.SEVERE, "Failed to download evaluator file resources: {0}. {1}",
            new Object[]{fileResourcesUrl, e});
        throw new RuntimeException(e);
      }
    } else {
      LOG.log(Level.FINER, "No file resources URL given.");
    }

    File evaluatorConfigurationFile = new File(this.reefFileNames.getEvaluatorConfigurationPath());
    LOG.log(Level.FINER, "Persisting evaluator config at: {0}", evaluatorConfigurationFile.getAbsolutePath());

    try {
      boolean newFileCreated = evaluatorConfigurationFile.createNewFile();
      LOG.log(Level.FINEST,
          newFileCreated ? "Created a new file for persisting evaluator configuration at {0}."
              : "Using existing file for persisting evaluator configuration at {0}.",
          evaluatorConfigurationFile.getAbsolutePath());

      Configuration evaluatorConfiguration = this.configurationSerializer.fromString(evaluatorConfigString);
      this.configurationSerializer.toFile(evaluatorConfiguration, evaluatorConfigurationFile);
    } catch (final IOException | BindException e) {
      LOG.log(Level.SEVERE, "An unexpected exception occurred while attempting to deserialize and write " +
          "Evaluator configuration file. {0}", e);
      throw new RuntimeException("Unable to write configuration.", e);
    }

    LOG.log(Level.INFO, "Launching the evaluator by invoking the following command: " + launchCommand);

    try {
      final List<String> command = Arrays.asList(launchCommand.split(" "));
      this.evaluatorProcess = new ProcessBuilder()
          .command(command)
          .redirectError(new File(this.azureBatchFileNames.getEvaluatorStdErrFilename()))
          .redirectOutput(new File(this.azureBatchFileNames.getEvaluatorStdOutFilename()))
          .start();

      // This will block the current thread until the Evaluator process completes.
      this.evaluatorProcessExitValue = EvaluatorShim.this.evaluatorProcess.waitFor();
      LOG.log(Level.INFO, "Evaluator process completed with exit value: {0}.", this.evaluatorProcessExitValue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    LOG.log(Level.FINEST, "Exiting EvaluatorShim.onEvaluatorLaunch().");
  }

  private File downloadFile(final String url) throws IOException, StorageException {
    URI fileUri = URI.create(url);
    File downloadedFile = new File(this.azureBatchFileNames.getEvaluatorResourceFilesJarName());
    LOG.log(Level.FINE, "Downloading evaluator file resources to {0}.", downloadedFile.getAbsolutePath());

    try (FileOutputStream fileStream = new FileOutputStream(downloadedFile)) {
      CloudBlob blob = new CloudBlockBlob(fileUri);
      blob.download(fileStream);
    }

    return downloadedFile;
  }

  private void extractFiles(final File zipFile) throws IOException {
    try (ZipFile zipFileHandle = new ZipFile(zipFile)) {
      Enumeration<? extends ZipEntry> zipEntries = zipFileHandle.entries();
      Path reefPath = this.reefFileNames.getREEFFolder().toPath();
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = zipEntries.nextElement();
        Path destination = new File(this.reefFileNames.getREEFFolder(), zipEntry.getName()).toPath();
        if (!destination.startsWith(reefPath)) {
          throw new IOException("Trying to unzip a file outside of the destination folder: " + destination);
        }
        File file = destination.toFile();
        if (file.exists()) {
          LOG.log(Level.INFO, "Skipping entry {0} because the file already exists.", zipEntry.getName());
        } else {
          if (zipEntry.isDirectory()) {
            if (file.mkdirs()) {
              LOG.log(Level.INFO, "Creating directory {0}.", zipEntry.getName());
            } else {
              LOG.log(Level.INFO, "Directory {0} already exists. Ignoring.", zipEntry.getName());
            }
          } else {
            try (InputStream inputStream = zipFileHandle.getInputStream(zipEntry)) {
              LOG.log(Level.INFO, "Extracting {0}.", zipEntry.getName());
              Files.copy(inputStream, destination);
              LOG.log(Level.INFO, "Extracting {0} completed.", zipEntry.getName());
            }
          }
        }
      }
    }
  }
}
