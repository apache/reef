/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.yarn.master;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.DriverRuntimeProtocol.*;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceReleaseHandler;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.LaunchCommandBuilder;
import com.microsoft.reef.runtime.yarn.util.YarnUtils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class YarnContainerManager implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

  private static final Logger LOG = Logger.getLogger(YarnContainerManager.class.getName());

  private static final String RUNTIME_NAME = "YARN";

  private final String globalClassPath;

  private final RuntimeClock clock;

  private final Path jobSubmissionDirectory;

  private final YarnConfiguration yarnConf;

  private final YarnClient yarnClient;

  private final FileSystem fileSystem;

  private final Map<String, LocalResource> globalResources = new HashMap<>();

  private final AMRMClientAsync resourceManager;

  private final NMClientAsync nodeManager;

  private final EStage<ResourceAllocationProto> resourceAllocationHandler;

  private final EventHandler<ResourceStatusProto> resourceStatusHandler;

  private final EventHandler<RuntimeStatusProto> runtimeStatusHandlerEventHandler;

  private final EventHandler<NodeDescriptorProto> nodeDescriptorProtoEventHandler;

  private final Map<String, Container> allocatedContainers = new ConcurrentHashMap<>();

  private RegisterApplicationMasterResponse registration;

  private int requestedContainerCount = 0;

  @Inject
  YarnContainerManager(
      final RuntimeClock clock,
      final YarnConfiguration yarnConf,
      final @Parameter(YarnMasterConfiguration.GlobalFileClassPath.class) String globalClassPath,
      final @Parameter(YarnMasterConfiguration.YarnHeartbeatPeriod.class) int yarnRMHeartbeatPeriod,
      final @Parameter(YarnMasterConfiguration.JobSubmissionDirectory.class) String jobSubmissionDirectory,
      final @Parameter(RuntimeParameters.NodeDescriptorHandler.class) EventHandler<NodeDescriptorProto> nodeDescriptorProtoEventHandler,
      final @Parameter(RuntimeParameters.RuntimeStatusHandler.class) EventHandler<RuntimeStatusProto> runtimeStatusProtoEventHandler,
      final @Parameter(RuntimeParameters.ResourceAllocationHandler.class) EventHandler<ResourceAllocationProto> resourceAllocationHandler,
      final @Parameter(RuntimeParameters.ResourceStatusHandler.class) EventHandler<ResourceStatusProto> resourceStatusHandler)
  throws IOException {

    this.globalClassPath = globalClassPath;
    this.clock = clock;
    this.jobSubmissionDirectory = new Path(jobSubmissionDirectory);
    this.yarnConf = yarnConf;
    this.resourceAllocationHandler = new ThreadPoolStage<>(resourceAllocationHandler, 8);
    this.resourceStatusHandler = resourceStatusHandler;
    this.runtimeStatusHandlerEventHandler = runtimeStatusProtoEventHandler;
    this.nodeDescriptorProtoEventHandler = nodeDescriptorProtoEventHandler;

    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConf);

    this.fileSystem = FileSystem.get(this.yarnConf);

    // GLOBAL FILE RESOURCES
    final Path globalFilePath =
        new Path(this.jobSubmissionDirectory, YarnMasterConfiguration.GLOBAL_FILE_DIRECTORY);

    if (this.fileSystem.exists(globalFilePath)) {
      final FileContext fileContext = FileContext.getFileContext(this.fileSystem.getUri());
      setResources(this.fileSystem, this.globalResources, fileContext.listStatus(globalFilePath));
    }

    this.resourceManager = AMRMClientAsync.createAMRMClientAsync(yarnRMHeartbeatPeriod, this);
    this.nodeManager = new NMClientAsyncImpl(this);
  }

  @Override
  public final void onContainersCompleted(final List<ContainerStatus> containerStatuses) {
    for (final ContainerStatus containerStatus : containerStatuses) {
      handle(containerStatus);
    }
  }

  @Override
  public final void onContainersAllocated(final List<Container> containers) {

    // ID is used for logging only
    final String id = String.format("%s:%d",
        Thread.currentThread().getName().replace(' ', '_'), System.currentTimeMillis());

    LOG.log(Level.FINE, "TIME: Allocated Containers {0} {1} of {2}",
        new Object[] { id, containers.size(), this.requestedContainerCount });

    for (final Container container : containers) {
      handleNewContainer(container);
    }

    LOG.log(Level.FINE, "TIME: Processed Containers {0}", id);
  }

  @Override
  public void onShutdownRequest() {
    this.clock.stop();
    this.runtimeStatusHandlerEventHandler.onNext(RuntimeStatusProto.newBuilder()
        .setName(RUNTIME_NAME).setState(ReefServiceProtos.State.DONE).build());
  }

  @Override
  public void onNodesUpdated(final List<NodeReport> nodeReports) {
    for (final NodeReport nodeReport : nodeReports) {
      handle(nodeReport);
    }
  }

  @Override
  public final float getProgress() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public final void onError(final Throwable throwable) {
    onRuntimeError(throwable);
  }

  @Override
  public final void onContainerStarted(final ContainerId containerId,
                                       final Map<String, ByteBuffer> stringByteBufferMap) {
    final Container container;
    synchronized (this.allocatedContainers) {
      container = this.allocatedContainers.get(containerId.toString());
    }

    if (container != null) {
      nodeManager.getContainerStatusAsync(containerId, container.getNodeId());
    }
  }

  @Override
  public final void onContainerStatusReceived(
      final ContainerId containerId, final ContainerStatus containerStatus) {
    handle(containerStatus);
  }

  @Override
  public final void onContainerStopped(final ContainerId containerId) {

    final boolean hasContainer;
    synchronized (this.allocatedContainers) {
      hasContainer = this.allocatedContainers.containsKey(containerId.toString());
    }

    if (hasContainer) {
      final ResourceStatusProto.Builder resourceStatusBuilder =
          ResourceStatusProto.newBuilder().setIdentifier(containerId.toString());
      resourceStatusBuilder.setState(ReefServiceProtos.State.DONE);
      this.resourceStatusHandler.onNext(resourceStatusBuilder.build());
    }
  }

  @Override
  public final void onStartContainerError(final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public final void onGetContainerStatusError(final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public final void onStopContainerError(final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // HELPER METHODS

  private void handle(final NodeReport nodeReport) {
    LOG.log(Level.FINE, "Send node descriptor: {0}", nodeReport);
    this.nodeDescriptorProtoEventHandler.onNext(NodeDescriptorProto.newBuilder()
        .setIdentifier(nodeReport.getNodeId().toString())
        .setHostName(nodeReport.getNodeId().getHost())
        .setPort(nodeReport.getNodeId().getPort())
        .setMemorySize(nodeReport.getCapability().getMemory())
        .setRackName(nodeReport.getRackName())
        .build());
  }

  private void handleContainerError(final ContainerId containerId, final Throwable throwable) {

    final ResourceStatusProto.Builder resourceStatusBuilder =
        ResourceStatusProto.newBuilder().setIdentifier(containerId.toString());

    resourceStatusBuilder.setState(ReefServiceProtos.State.FAILED);
    resourceStatusBuilder.setExitCode(1);
    resourceStatusBuilder.setDiagnostics(throwable.getMessage());

    this.resourceStatusHandler.onNext(resourceStatusBuilder.build());
  }

  /**
   * Handles new container allocations. Calls come from YARN.
   *
   * @param container newly allocated
   */
  private void handleNewContainer(final Container container) {

    LOG.log(Level.FINE, "New allocated container: id[ {0} ]", container.getId());
    synchronized (this.allocatedContainers) {
      this.allocatedContainers.put(container.getId().toString(), container);
    }

    --this.requestedContainerCount;

    final ResourceAllocationProto allocation =
        ResourceAllocationProto.newBuilder()
            .setIdentifier(container.getId().toString())
            .setNodeId(container.getNodeId().toString())
            .setResourceMemory(container.getResource().getMemory())
            .build();
    this.resourceAllocationHandler.onNext(allocation);
    updateRuntimeStatus();
  }

  /**
   * Handles container status reports. Calls come from YARN.
   *
   * @param value containing the container status
   */
  private void handle(final ContainerStatus value) {

    final boolean hasContainer;
    synchronized (this.allocatedContainers) {
      hasContainer = this.allocatedContainers.containsKey(value.getContainerId().toString());
    }

    if (hasContainer) {
      LOG.log(Level.FINE, "Received container status: {0}", value.getContainerId());

      final ResourceStatusProto.Builder status =
          ResourceStatusProto.newBuilder().setIdentifier(value.getContainerId().toString());

      switch (value.getState()) {
        case COMPLETE:
          LOG.info("container complete");
          status.setState(ReefServiceProtos.State.DONE);
          status.setExitCode(value.getExitStatus());
          break;
        default:
          LOG.info("container running");
          status.setState(ReefServiceProtos.State.RUNNING);
      }

      if (value.getDiagnostics() != null) {
        LOG.log(Level.FINE, "Container diagnostics: {0}", value.getDiagnostics());
        status.setDiagnostics(value.getDiagnostics());
      }

      this.resourceStatusHandler.onNext(status.build());
    }
  }

  private void handle(final ResourceLaunchProto resourceLaunchProto) {

    try {

      final String containerId = resourceLaunchProto.getIdentifier();
      LOG.log(Level.FINEST, "TIME: Start ResourceLaunchProto {0}", containerId);

      final Container container;
      synchronized (this.allocatedContainers) {
        container = this.allocatedContainers.get(containerId);
        if (container == null) {
          LOG.log(Level.SEVERE, "Unknown allocated container identifier: {0} of {1}",
                  new Object[] { containerId, this.allocatedContainers.keySet() });
          throw new RuntimeException("Unknown allocated container identifier: " + containerId);
        }
      }

      LOG.log(Level.FINEST, "Setting up container launch container for id={0}", container.getId());

      final Path evaluatorSubmissionDirectory = new Path(this.jobSubmissionDirectory, container.getId().toString());
      final Map<String, LocalResource> localResources = new HashMap<>(this.globalResources);

      // EVALUATOR CONFIGURATION
      final File evaluatorConfigurationFile = File.createTempFile("evaluator_" + container.getId(), ".conf");
      LOG.log(Level.FINEST, "TIME: Config ResourceLaunchProto {0} {1}",
              new Object[] { containerId, evaluatorConfigurationFile });

      FileUtils.writeStringToFile(evaluatorConfigurationFile, resourceLaunchProto.getEvaluatorConf());
      localResources.put(evaluatorConfigurationFile.getName(),
          YarnUtils.getLocalResource(this.fileSystem, new Path(evaluatorConfigurationFile.toURI()),
              new Path(evaluatorSubmissionDirectory, evaluatorConfigurationFile.getName())));

      // LOCAL FILE RESOURCES
      LOG.log(Level.FINEST, "TIME: Local ResourceLaunchProto {0}", containerId);
      final StringBuilder localClassPath = new StringBuilder();
      for (final ReefServiceProtos.FileResourceProto file : resourceLaunchProto.getFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(this.jobSubmissionDirectory, file.getName());
        switch (file.getType()) {
          case PLAIN:
            if (this.fileSystem.exists(dst)) {
              LOG.log(Level.FINEST, "LOCAL FILE RESOURCE: reference {0}", dst);
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, dst));
            } else {
              LOG.log(Level.FINEST, "LOCAL FILE RESOURCE: upload {0} to {1}", new Object[] { src, dst });
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            }
            break;
          case LIB:
            localClassPath.append(File.pathSeparatorChar + file.getName());
            if (this.fileSystem.exists(dst)) {
              LOG.log(Level.FINEST, "LOCAL LIB FILE RESOURCE: reference {0}", dst);
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, dst));
            } else {
              LOG.log(Level.FINEST, "LOCAL LIB FILE RESOURCE: upload {0} to {1}", new Object[] { src, dst });
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            }

            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            break;
        }
      }

      final String classPath = localClassPath.toString().isEmpty() ?
          this.globalClassPath : localClassPath.toString() + File.pathSeparatorChar + this.globalClassPath;

      final LaunchCommandBuilder commandBuilder;
      switch (resourceLaunchProto.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder().setClassPath(classPath);
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException("Unsupported container type: " + resourceLaunchProto.getType());
      }

      final List<String> commandList = commandBuilder
          .setErrorHandlerRID(resourceLaunchProto.getRemoteId())
          .setLaunchID(resourceLaunchProto.getIdentifier())
          .setConfigurationFileName(evaluatorConfigurationFile.getName())
          .setMemory(container.getResource().getMemory())
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/evaluator.stderr")
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/evaluator.stdout")
          .build();

      final String command = StringUtils.join(commandList, ' ');
      LOG.log(Level.FINEST, "TIME: Run ResourceLaunchProto {0} command: `{1}` with resources: `{2}`",
          new Object[] { containerId, command, localResources });

      final ContainerLaunchContext ctx = YarnUtils.getContainerLaunchContext(command, localResources);
      nodeManager.startContainerAsync(container, ctx);

      LOG.log(Level.FINEST, "TIME: End ResourceLaunchProto {0}", containerId);

    } catch (final Throwable e) {
      LOG.log(Level.WARNING, "Error handling resource launch message: " + resourceLaunchProto, e);
      throw new RuntimeException(e);
    }
  }

  private void setResources(final FileSystem fs,
                            final Map<String, LocalResource> resources,
                            final RemoteIterator<FileStatus> files) throws IOException {
    while (files.hasNext()) {
      final FileStatus fstatus = files.next();
      if (fstatus.isFile()) {
        LOG.log(Level.FINE, "Load file resource: {0}", fstatus.getPath());
        resources.put(fstatus.getPath().getName(), YarnUtils.getLocalResource(fs, fstatus.getPath()));
      } else if (fstatus.isSymlink()) {
        LOG.log(Level.FINE, "Load symlink resource: {0}", fstatus.getSymlink());
        resources.put(fstatus.getPath().getName(), YarnUtils.getLocalResource(fs, fstatus.getSymlink()));
      }
    }
  }

  private void handle(final ResourceRequestProto resourceRequestProto) {

    final ResourceRequest request = Records.newRecord(ResourceRequest.class);

    final String[] nodes = resourceRequestProto.getNodeNameCount() == 0 ? null :
        resourceRequestProto.getNodeNameList().toArray(new String[resourceRequestProto.getNodeNameCount()]);
    final String[] racks = resourceRequestProto.getRackNameCount() == 0 ? null :
        resourceRequestProto.getRackNameList().toArray(new String[resourceRequestProto.getRackNameCount()]);

    // set the priority for the request
    final Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(resourceRequestProto.hasPriority() ? resourceRequestProto.getPriority() : 1);

    org.apache.hadoop.yarn.api.records.Resource capability =
        Records.newRecord(org.apache.hadoop.yarn.api.records.Resource.class);

    final int memory = YarnUtils.getMemorySize(resourceRequestProto.getResourceSize(),
        512, registration.getMaximumResourceCapability().getMemory());

    LOG.log(Level.FINE, "Request memory: {0} MB", memory);
    capability.setMemory(memory);
    request.setCapability(capability);

    final boolean relax_locality =
        !resourceRequestProto.hasRelaxLocality() || resourceRequestProto.getRelaxLocality();

    for (int i = 0; i < resourceRequestProto.getResourceCount(); i++) {
      this.resourceManager.addContainerRequest(
          new AMRMClient.ContainerRequest(capability, nodes, racks, pri, relax_locality));
    }

    this.requestedContainerCount += resourceRequestProto.getResourceCount();
  }

  /**
   * Update the driver with my current status
   */
  private void updateRuntimeStatus() {

    final DriverRuntimeProtocol.RuntimeStatusProto.Builder builder =
        DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
            .setName(RUNTIME_NAME)
            .setState(ReefServiceProtos.State.RUNNING)
            .setOutstandingContainerRequests(this.requestedContainerCount);

    synchronized (this.allocatedContainers) {
      for (final Container allocated : this.allocatedContainers.values()) {
        builder.addContainerAllocation(allocated.getId().toString());
      }
    }

    this.runtimeStatusHandlerEventHandler.onNext(builder.build());
  }

  private void onRuntimeError(final Throwable throwable) {
    // SHUTDOWN YARN
    try {
      resourceAllocationHandler.close();
      resourceManager.unregisterApplicationMaster(FinalApplicationStatus.FAILED, throwable.getMessage(), null);
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Error shutting down YARN application", e);
    } finally {
      resourceManager.stop();
    }

    final RuntimeStatusProto.Builder runtimeStatusBuilder = RuntimeStatusProto.newBuilder()
        .setState(ReefServiceProtos.State.FAILED)
        .setName("YARN 2.1");

    if (throwable instanceof Serializable) {
      final ObjectSerializableCodec<Throwable> codec = new ObjectSerializableCodec<>();
      runtimeStatusBuilder.setError(ReefServiceProtos.RuntimeErrorProto.newBuilder()
          .setName("YARN 2.1")
          .setMessage(throwable.getMessage())
          .setException(ByteString.copyFrom(codec.encode(throwable)))
          .build())
          .build();
    } else {
      LOG.log(Level.WARNING, "Exception not serializable", throwable);
      runtimeStatusBuilder.setError(ReefServiceProtos.RuntimeErrorProto.newBuilder()
          .setName("YARN 2.1")
          .setMessage(throwable.getMessage())
          .build())
          .build();
    }

    this.runtimeStatusHandlerEventHandler.onNext(runtimeStatusBuilder.build());
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // CLOCK EVENTS

  final class RuntimeStartHander implements EventHandler<RuntimeStart> {

    @Override
    public void onNext(final RuntimeStart runtimeStart) {
      try {
        yarnClient.start();
        for (final NodeReport nodeReport : yarnClient.getNodeReports(NodeState.RUNNING)) {
          handle(nodeReport);
        }

        resourceManager.init(yarnConf);
        resourceManager.start();

        nodeManager.init(yarnConf);
        nodeManager.start();
        registration = resourceManager.registerApplicationMaster("", 0, "");
      } catch (final YarnException | IOException e) {
        LOG.log(Level.WARNING, "Error starting YARN Node Manager", e);
        onRuntimeError(e);
      }
    }
  }

  final class RuntimeStopHandler implements EventHandler<RuntimeStop> {
    @Override
    public void onNext(final RuntimeStop runtimeStop) {
      LOG.log(Level.FINE, "Stop Runtime: RM status {0}", resourceManager.getServiceState());
      if (resourceManager.getServiceState() == Service.STATE.STARTED) {
        // invariant: if RM is still running then we declare success.
        try {
          resourceAllocationHandler.close();
          resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
          resourceManager.close();
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "Error shutting down YARN application", e);
        }
      }

      if (nodeManager.getServiceState() == Service.STATE.STARTED) {
        try {
          nodeManager.close();
        } catch (final IOException e) {
          LOG.log(Level.WARNING, "Error closing YARN Node Manager", e);
        }
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // EVENT RELAY CLASSES

  final class ResourceLaunchHandlerImpl implements ResourceLaunchHandler {
    @Override
    public void onNext(final ResourceLaunchProto resourceLaunchProto) {
      handle(resourceLaunchProto);
    }
  }

  final class ResourceReleaseHandlerImpl implements ResourceReleaseHandler {

    @Override
    public void onNext(final ResourceReleaseProto resourceReleaseProto) {

      final String containerId = resourceReleaseProto.getIdentifier();
      LOG.log(Level.FINE, "Release container: {0}", containerId);

      final Container container;
      synchronized (allocatedContainers) {
        container = allocatedContainers.remove(containerId);
        if (container == null) {
          LOG.log(Level.SEVERE, "Unknown allocated container identifier: {0} of {1}",
                  new Object[] { containerId, allocatedContainers.keySet() });
          throw new RuntimeException("Unknown allocated container identifier: " + containerId);
        }
      }

      resourceManager.releaseAssignedContainer(container.getId());
      updateRuntimeStatus();
    }
  }

  final class ResourceRequestHandlerImpl implements ResourceRequestHandler {
    @Override
    public void onNext(final ResourceRequestProto resourceRequestProto) {
      handle(resourceRequestProto);
      updateRuntimeStatus();
    }
  }
}
