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
package org.apache.reef.runtime.azbatch.driver;

import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.TaskState;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.proto.EvaluatorShimProtocol;
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.azbatch.util.batch.AzureBatchHelper;
import org.apache.reef.runtime.azbatch.util.storage.AzureStorageClient;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.azbatch.util.RemoteIdentifierParser;
import org.apache.reef.runtime.azbatch.util.TaskStatusMapper;
import org.apache.reef.runtime.common.driver.api.ResourceLaunchEvent;
import org.apache.reef.runtime.common.driver.api.ResourceReleaseEvent;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.common.files.*;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.impl.SocketRemoteIdentifier;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.runtime.azbatch.driver.RuntimeIdentifier.RUNTIME_NAME;

/**
 * The Driver's view of evaluator shims running in the cluster. This class serves the following purposes:
 *    1. listens for evaluator shim status messages signaling that the shim is online and ready to start the evaluator
 *    process.
 *    2. listens for {@link ResourceLaunchEvent} events and sends commands to the evaluator shims to start the
 *    evaluator process.
 *    3. listens for {@link ResourceReleaseEvent} events and sends terminate commands to the evaluator shims.
 *    4. triggers {@link org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent}
 *       events to update REEF Common on runtime status.
 *    5. triggers {@link org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent}
 *       events to update REEF Common on container statuses.
 */
@Private
@DriverSide
public final class AzureBatchEvaluatorShimManager
    implements EventHandler<RemoteMessage<EvaluatorShimProtocol.EvaluatorShimStatusProto>> {

  private static final Logger LOG = Logger.getLogger(AzureBatchEvaluatorShimManager.class.getName());

  private static final int EVALUATOR_SHIM_MEMORY_MB = 64;

  private final Map<String, ResourceRequestEvent> outstandingResourceRequests;
  private final AtomicInteger outstandingResourceRequestCount;

  private final Map<String, CloudTask> failedResources;

  private final AutoCloseable evaluatorShimCommandChannel;

  private final AzureStorageClient azureStorageClient;
  private final REEFFileNames reefFileNames;
  private final AzureBatchFileNames azureBatchFileNames;
  private final RemoteManager remoteManager;
  private final AzureBatchHelper azureBatchHelper;
  private final AzureBatchEvaluatorShimConfigurationProvider evaluatorShimConfigurationProvider;
  private final JobJarMaker jobJarMaker;
  private final CommandBuilder launchCommandBuilder;
  private final REEFEventHandlers reefEventHandlers;
  private final ConfigurationSerializer configurationSerializer;

  private final Evaluators evaluators;

  @Inject
  AzureBatchEvaluatorShimManager(
      final AzureStorageClient azureStorageClient,
      final REEFFileNames reefFileNames,
      final AzureBatchFileNames azureBatchFileNames,
      final RemoteManager remoteManager,
      final REEFEventHandlers reefEventHandlers,
      final Evaluators evaluators,
      final CommandBuilder launchCommandBuilder,
      final AzureBatchHelper azureBatchHelper,
      final JobJarMaker jobJarMaker,
      final AzureBatchEvaluatorShimConfigurationProvider evaluatorShimConfigurationProvider,
      final ConfigurationSerializer configurationSerializer) {
    this.azureStorageClient = azureStorageClient;
    this.reefFileNames = reefFileNames;
    this.azureBatchFileNames = azureBatchFileNames;
    this.remoteManager = remoteManager;

    this.reefEventHandlers = reefEventHandlers;
    this.evaluators = evaluators;

    this.launchCommandBuilder = launchCommandBuilder;

    this.azureBatchHelper = azureBatchHelper;
    this.jobJarMaker = jobJarMaker;

    this.evaluatorShimConfigurationProvider = evaluatorShimConfigurationProvider;

    this.outstandingResourceRequests = new ConcurrentHashMap<>();
    this.outstandingResourceRequestCount = new AtomicInteger();

    this.failedResources = new ConcurrentHashMap<>();

    this.evaluatorShimCommandChannel = remoteManager
        .registerHandler(EvaluatorShimProtocol.EvaluatorShimStatusProto.class, this);

    this.configurationSerializer = configurationSerializer;
  }

  /**
   * This method is called when a resource is requested. It will add a task to the existing Azure Batch job which
   * is equivalent to requesting a container in Azure Batch. When the request is fulfilled and the evaluator shim is
   * started, it will send a message back to the driver which signals that a resource request was fulfilled.
   *
   * @param resourceRequestEvent resource request event.
   * @param containerId container id for the resource. It will be used as the task id for Azure Batch task.
   * @param jarFileUri Azure Storage SAS URI of the JAR file containing libraries required by the evaluator shim.
   */
  public void onResourceRequested(final ResourceRequestEvent resourceRequestEvent,
                                  final String containerId,
                                  final URI jarFileUri) {
    try {
      createAzureBatchTask(containerId, jarFileUri);
      this.outstandingResourceRequests.put(containerId, resourceRequestEvent);
      this.outstandingResourceRequestCount.incrementAndGet();
      this.updateRuntimeStatus();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create Azure Batch task with the following exception: {0}", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is invoked by the RemoteManager when a message from the evaluator shim is received.
   *
   * @param statusMessage the message from the evaluator shim indicating that the shim has started and is ready to
   *                      start the evaluator process.
   */
  @Override
  public void onNext(final RemoteMessage<EvaluatorShimProtocol.EvaluatorShimStatusProto> statusMessage) {

    EvaluatorShimProtocol.EvaluatorShimStatusProto message = statusMessage.getMessage();
    String containerId = message.getContainerId();
    String remoteId = message.getRemoteIdentifier();

    LOG.log(Level.INFO, "Got a status message from evaluator shim = {0} with containerId = {1} and status = {2}.",
        new String[]{remoteId, containerId, message.getStatus().toString()});

    if (message.getStatus() != EvaluatorShimProtocol.EvaluatorShimStatus.ONLINE) {
      LOG.log(Level.SEVERE, "Unexpected status returned from the evaluator shim: {0}. Ignoring the message.",
          message.getStatus().toString());
      return;
    }

    this.onResourceAllocated(containerId, remoteId, Optional.<CloudTask>empty());
  }

  /**
   * This method is invoked when the Azure Batch runtime is notified that a pending resource request has been
   * fulfilled. It could happen because of two reasons:
   *    1. The driver receives a message from the evaluator shim indicating it has successfully started.
   *    2. {@link AzureBatchTaskStatusAlarmHandler} detects that the evaluator shim failed before sending the status
   *       message.
   *
   * @param containerId id of the container.
   * @param remoteId remote address for the allocated container.
   * @param cloudTask Azure Batch task which corresponds to the container.
   */
  public void onResourceAllocated(final String containerId,
                                  final String remoteId,
                                  final Optional<CloudTask> cloudTask) {
    ResourceRequestEvent resourceRequestEvent = this.outstandingResourceRequests.remove(containerId);

    if (resourceRequestEvent == null) {
      LOG.log(Level.WARNING, "No outstanding resource request found for container id = {0}.", containerId);
    } else {
      this.outstandingResourceRequestCount.decrementAndGet();

      // We would expect the Azure Batch task to be in 'RUNNING' state. If it is in
      // 'COMPLETED' state, it cannot receiver instructions and thus by definition
      // has failed.
      if (cloudTask.isPresent() && TaskState.COMPLETED.equals(cloudTask.get().state())) {
        this.failedResources.put(containerId, cloudTask.get());
      }

      LOG.log(Level.FINEST, "Notifying REEF of a new node: {0}", remoteId);
      this.reefEventHandlers.onNodeDescriptor(NodeDescriptorEventImpl.newBuilder()
          .setIdentifier(RemoteIdentifierParser.parseNodeId(remoteId))
          .setHostName(RemoteIdentifierParser.parseIp(remoteId))
          .setPort(RemoteIdentifierParser.parsePort(remoteId))
          .setMemorySize(resourceRequestEvent.getMemorySize().get())
          .build());

      LOG.log(Level.FINEST, "Triggering a new ResourceAllocationEvent for remoteId = {0}.", remoteId);
      this.reefEventHandlers.onResourceAllocation(
          ResourceEventImpl.newAllocationBuilder()
              .setIdentifier(containerId)
              .setNodeId(RemoteIdentifierParser.parseNodeId(remoteId))
              .setResourceMemory(resourceRequestEvent.getMemorySize().get())
              .setVirtualCores(resourceRequestEvent.getVirtualCores().get())
              .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
              .build());
    }

    this.updateRuntimeStatus();
  }

  /**
   * Event handler method for {@link ResourceLaunchEvent}. This method will determine if the evaluator shim
   * is online and send the evaluator launch command to the shim to start the evaluator.
   *
   * @param resourceLaunchEvent an instance of {@ResourceLaunchEvent}
   * @param command OS command to launch the evaluator process.
   * @param evaluatorConfigurationString evaluator configuration serialized as a String.
   */
  public void onResourceLaunched(final ResourceLaunchEvent resourceLaunchEvent,
                                 final String command,
                                 final String evaluatorConfigurationString) {
    final String resourceId = resourceLaunchEvent.getIdentifier();

    if (this.failedResources.containsKey(resourceId)) {
      LOG.log(Level.FINE, "ResourceLaunch event triggered on a failed container. " +
          "Notifying REEF of failed container.");
      CloudTask cloudTask = this.failedResources.get(resourceId);
      this.onAzureBatchTaskStatus(cloudTask);
    } else if (this.evaluators.get(resourceId).isPresent()) {
      LOG.log(Level.FINE, "Preparing to launch resourceId = {0}", resourceId);
      this.launchEvaluator(resourceLaunchEvent, command, evaluatorConfigurationString);
    } else {
      LOG.log(Level.WARNING, "Received a ResourceLaunch event for an unknown resourceId = {0}", resourceId);
    }

    this.updateRuntimeStatus();
  }

  /**
   * Event handler method for {@link ResourceReleaseEvent}. Sends a TERMINATE command to the appropriate evaluator shim.
   *
   * @param resourceReleaseEvent
   */
  public void onResourceReleased(final ResourceReleaseEvent resourceReleaseEvent) {
    String resourceRemoteId = getResourceRemoteId(resourceReleaseEvent.getIdentifier());

    // REEF Common will trigger a ResourceReleaseEvent even if the resource has failed. Since we know that the shim
    // has already failed, we can safely ignore this.
    if (this.failedResources.remove(resourceReleaseEvent.getIdentifier()) != null) {
      LOG.log(Level.INFO, "Received a ResourceReleaseEvent for a failed shim with resourceId = {0}. Ignoring.",
          resourceReleaseEvent.getIdentifier());
    } else if (this.evaluators.get(resourceReleaseEvent.getIdentifier()).isPresent()) {
      EventHandler<EvaluatorShimProtocol.EvaluatorShimControlProto> handler =
          this.remoteManager.getHandler(resourceRemoteId, EvaluatorShimProtocol.EvaluatorShimControlProto.class);

      LOG.log(Level.INFO, "Sending TERMINATE command to the shim with remoteId = {0}.", resourceRemoteId);
      handler.onNext(
          EvaluatorShimProtocol.EvaluatorShimControlProto
              .newBuilder()
              .setCommand(EvaluatorShimProtocol.EvaluatorShimCommand.TERMINATE)
              .build());

      this.updateRuntimeStatus();
    }
  }

  /**
   * Takes in an instance of {@link CloudTask}, generates and triggers a
   * {@link org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent}.
   *
   * @param cloudTask and instance of {@link CloudTask}.
   */
  public void onAzureBatchTaskStatus(final CloudTask cloudTask) {
    ResourceStatusEventImpl.Builder eventBuilder =
        ResourceStatusEventImpl.newBuilder()
            .setIdentifier(cloudTask.id())
            .setState(TaskStatusMapper.getReefTaskState(cloudTask))
            .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME);

    if (TaskState.COMPLETED.equals(cloudTask.state())) {
      eventBuilder.setExitCode(cloudTask.executionInfo().exitCode());
    }

    this.reefEventHandlers.onResourceStatus(eventBuilder.build());
  }

  /**
   * Closes the evaluator shim remote manager command channel.
   */
  public void onClose() {
    try {
      this.evaluatorShimCommandChannel.close();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "An unexpected exception while closing the Evaluator Shim Command channel: {0}", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * A utility method which builds the evaluator shim JAR file and uploads it to Azure Storage.
   *
   * @return SAS URI to where the evaluator shim JAR was uploaded.
   */
  public URI generateShimJarFile() {

    try {
      Set<FileResource> globalFiles = new HashSet<>();

      final File globalFolder = new File(this.reefFileNames.getGlobalFolderPath());
      final File[] filesInGlobalFolder = globalFolder.listFiles();

      for (final File fileEntry : filesInGlobalFolder != null ? filesInGlobalFolder : new File[]{}) {
        globalFiles.add(getFileResourceFromFile(fileEntry, FileType.LIB));
      }

      File jarFile = this.jobJarMaker.newBuilder()
          .addGlobalFileSet(globalFiles)
          .build();

      return uploadFile(jarFile);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to build JAR file", ex);
      throw new RuntimeException(ex);
    }
  }

  private void updateRuntimeStatus() {
    this.reefEventHandlers.onRuntimeStatus(RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME)
        .setState(State.RUNNING)
        .setOutstandingContainerRequests(this.outstandingResourceRequestCount.get())
        .build());
  }

  private void launchEvaluator(final ResourceLaunchEvent resourceLaunchEvent,
                               final String command,
                               final String evaluatorConfigurationString) {
    String resourceId = resourceLaunchEvent.getIdentifier();
    String resourceRemoteId = getResourceRemoteId(resourceId);

    Set<FileResource> fileResources = resourceLaunchEvent.getFileSet();
    String fileUrl = "";
    if (!fileResources.isEmpty()) {
      try {
        File jarFile = writeFileResourcesJarFile(fileResources);
        fileUrl = uploadFile(jarFile).toString();
        LOG.log(Level.FINE, "Uploaded evaluator file resources to Azure Storage at {0}.", fileUrl);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to generate zip archive for evaluator file resources: {0}.", e);
        throw new RuntimeException(e);
      }
    } else {
      LOG.log(Level.INFO, "No file resources found in ResourceLaunchEvent.");
    }

    LOG.log(Level.INFO, "Sending a command to the Evaluator shim with remoteId = {0} to start the evaluator.",
        resourceRemoteId);
    EventHandler<EvaluatorShimProtocol.EvaluatorShimControlProto> handler = this.remoteManager
        .getHandler(resourceRemoteId, EvaluatorShimProtocol.EvaluatorShimControlProto.class);

    handler.onNext(
        EvaluatorShimProtocol.EvaluatorShimControlProto
            .newBuilder()
            .setCommand(EvaluatorShimProtocol.EvaluatorShimCommand.LAUNCH_EVALUATOR)
            .setEvaluatorLaunchCommand(command)
            .setEvaluatorConfigString(evaluatorConfigurationString)
            .setEvaluatorFileResourcesUrl(fileUrl)
            .build());
  }

  private String getEvaluatorShimLaunchCommand() {
    return this.launchCommandBuilder.buildEvaluatorShimCommand(EVALUATOR_SHIM_MEMORY_MB,
        this.azureBatchFileNames.getEvaluatorShimConfigurationPath());
  }

  /**
   * @return The name under which the evaluator shim configuration will be stored in
   * REEF_BASE_FOLDER/LOCAL_FOLDER.
   */
  private FileResource getFileResourceFromFile(final File configFile, final FileType type) {
    return FileResourceImpl.newBuilder()
        .setName(configFile.getName())
        .setPath(configFile.getPath())
        .setType(type).build();
  }

  private void createAzureBatchTask(final String taskId, final URI jarFileUri) throws IOException {
    final Configuration shimConfig = this.evaluatorShimConfigurationProvider.getConfiguration(taskId);
    final File shim = new File(this.reefFileNames.getLocalFolderPath(),
        taskId + '-' + this.azureBatchFileNames.getEvaluatorShimConfigurationName());
    this.configurationSerializer.toFile(shimConfig, shim);
    final URI shimUri = this.uploadFile(shim);
    this.azureBatchHelper.submitTask(this.azureBatchHelper.getAzureBatchJobId(), taskId, jarFileUri,
        shimUri, getEvaluatorShimLaunchCommand());
  }

  private File writeFileResourcesJarFile(final Set<FileResource> fileResourceSet) throws IOException {
    return this.jobJarMaker.newBuilder().addLocalFileSet(fileResourceSet).build();
  }

  private URI uploadFile(final File jarFile) throws IOException {
    final String folderName = this.azureBatchFileNames.getStorageJobFolder(this.azureBatchHelper.getAzureBatchJobId());
    LOG.log(Level.FINE, "Uploading {0} to {1}.", new Object[]{jarFile.getAbsolutePath(), folderName});

    return this.azureStorageClient.uploadFile(folderName, jarFile);
  }

  private String getResourceRemoteId(final String resourceId) {
    Optional<EvaluatorManager> optionalEvaluatorManager = this.evaluators.get(resourceId);

    if (!optionalEvaluatorManager.isPresent()) {
      LOG.log(Level.SEVERE, "Unknown evaluator with resourceId = {0}", resourceId);
      throw new RuntimeException("Unknown evaluator with resourceId = " + resourceId);
    }

    NodeDescriptor nodeDescriptor = optionalEvaluatorManager.get().getEvaluatorDescriptor().getNodeDescriptor();
    return (new SocketRemoteIdentifier(nodeDescriptor.getInetSocketAddress())).toString();
  }
}
