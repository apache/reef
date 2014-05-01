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
package com.microsoft.reef.runtime.yarn.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.DriverRuntimeProtocol.NodeDescriptorProto;
import com.microsoft.reef.proto.DriverRuntimeProtocol.ResourceAllocationProto;
import com.microsoft.reef.proto.DriverRuntimeProtocol.ResourceStatusProto;
import com.microsoft.reef.proto.DriverRuntimeProtocol.RuntimeStatusProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.runtime.RuntimeClock;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

final class YarnContainerManager implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

  private static final Logger LOG = Logger.getLogger(YarnContainerManager.class.getName());

  private static final String RUNTIME_NAME = "YARN";

  private final YarnConfiguration yarnConf;
  private final YarnClient yarnClient;
  private final AMRMClientAsync resourceManager;
  private final NMClientAsync nodeManager;
  private final REEFEventHandlers reefEventHandlers;
  private final Containers containers;
  private final ApplicationMasterRegistration registration;
  private final ContainerRequestCounter containerRequestCounter;
  private final DriverStatusManager driverStatusManager;
  private final TrackingURLProvider trackingURLProvider;

  @Inject
  YarnContainerManager(final RuntimeClock clock,
                       final YarnConfiguration yarnConf,
                       final @Parameter(YarnMasterConfiguration.YarnHeartbeatPeriod.class) int yarnRMHeartbeatPeriod,
                       final REEFEventHandlers reefEventHandlers,
                       final Containers containers,
                       final ApplicationMasterRegistration registration,
                       final ContainerRequestCounter containerRequestCounter,
                       final DriverStatusManager driverStatusManager,
                       final TrackingURLProvider trackingURLProvider) throws IOException {
    this.reefEventHandlers = reefEventHandlers;
    this.driverStatusManager = driverStatusManager;

    this.containers = containers;
    this.registration = registration;
    this.containerRequestCounter = containerRequestCounter;
    this.yarnConf = yarnConf;
    this.trackingURLProvider = trackingURLProvider;

    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConf);


    this.resourceManager = AMRMClientAsync.createAMRMClientAsync(yarnRMHeartbeatPeriod, this);
    this.nodeManager = new NMClientAsyncImpl(this);
    LOG.log(Level.FINEST, "Instantiated 'YarnContainerManager'");
  }

  @Override
  public final void onContainersCompleted(final List<ContainerStatus> containerStatuses) {
    for (final ContainerStatus containerStatus : containerStatuses) {
      onContainerStatus(containerStatus);
    }
  }

  @Override
  public final void onContainersAllocated(final List<Container> containers) {

    // ID is used for logging only
    final String id = String.format("%s:%d",
        Thread.currentThread().getName().replace(' ', '_'), System.currentTimeMillis());

    LOG.log(Level.FINE, "TIME: Allocated Containers {0} {1} of {2}",
        new Object[]{id, containers.size(), this.containerRequestCounter.get()});

    for (final Container container : containers) {
      handleNewContainer(container);
    }

    LOG.log(Level.FINE, "TIME: Processed Containers {0}", id);
  }

  @Override
  public void onShutdownRequest() {
    this.reefEventHandlers.onRuntimeStatus(RuntimeStatusProto.newBuilder()
        .setName(RUNTIME_NAME).setState(ReefServiceProtos.State.DONE).build());
    this.driverStatusManager.onError(new Exception("Shutdown requested by YARN."));
  }

  @Override
  public void onNodesUpdated(final List<NodeReport> nodeReports) {
    for (final NodeReport nodeReport : nodeReports) {
      onNodeReport(nodeReport);
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

    final Optional<Container> container = this.containers.getOptional(containerId.toString());

    if (container.isPresent()) {
      nodeManager.getContainerStatusAsync(containerId, container.get().getNodeId());
    }
  }

  @Override
  public final void onContainerStatusReceived(
      final ContainerId containerId, final ContainerStatus containerStatus) {
    onContainerStatus(containerStatus);
  }

  @Override
  public final void onContainerStopped(final ContainerId containerId) {

    final boolean hasContainer = this.containers.hasContainer(containerId.toString());
    if (hasContainer) {
      final ResourceStatusProto.Builder resourceStatusBuilder =
          ResourceStatusProto.newBuilder().setIdentifier(containerId.toString());
      resourceStatusBuilder.setState(ReefServiceProtos.State.DONE);
      this.reefEventHandlers.onResourceStatus(resourceStatusBuilder.build());
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

  /**
   * Submit the given launchContext to the given container.
   *
   * @param container
   * @param launchContext
   */
  void submit(final Container container, final ContainerLaunchContext launchContext) {
    this.nodeManager.startContainerAsync(container, launchContext);
  }

  /**
   * Release the given container.
   *
   * @param containerId
   */
  void release(final String containerId) {
    LOG.log(Level.FINE, "Release container: {0}", containerId);
    final Container container = containers.removeAndGet(containerId);
    resourceManager.releaseAssignedContainer(container.getId());
    updateRuntimeStatus();
  }


  synchronized void onStart() {
    try {
      yarnClient.start();
      for (final NodeReport nodeReport : yarnClient.getNodeReports(NodeState.RUNNING)) {
        onNodeReport(nodeReport);
      }

      this.resourceManager.init(this.yarnConf);
      this.resourceManager.start();

      this.nodeManager.init(yarnConf);
      this.nodeManager.start();
      this.registration.setRegistration(resourceManager.registerApplicationMaster("", 0, this.trackingURLProvider.getTrackingUrl()));
      LOG.log(Level.INFO, "YARN registration: {0}", registration);
    } catch (final YarnException | IOException e) {
      LOG.log(Level.WARNING, "Error starting YARN Node Manager", e);
      onRuntimeError(e);
    }
  }

  synchronized void onStop() {
    LOG.log(Level.FINE, "Stop Runtime: RM status {0}", resourceManager.getServiceState());
    if (resourceManager.getServiceState() == Service.STATE.STARTED) {
      // invariant: if RM is still running then we declare success.
      try {
        this.reefEventHandlers.close();
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

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // HELPER METHODS

  private void onNodeReport(final NodeReport nodeReport) {
    LOG.log(Level.FINE, "Send node descriptor: {0}", nodeReport);
    this.reefEventHandlers.onNodeDescriptor(NodeDescriptorProto.newBuilder()
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

    this.reefEventHandlers.onResourceStatus(resourceStatusBuilder.build());
  }

  /**
   * Handles new container allocations. Calls come from YARN.
   *
   * @param container newly allocated
   */
  private void handleNewContainer(final Container container) {

    LOG.log(Level.FINE, "New allocated container: id[ {0} ]", container.getId());
    synchronized (this.containers) {
      this.containers.add(container);
      this.containerRequestCounter.decrement();
    }
    this.reefEventHandlers.onResourceAllocation(ResourceAllocationProto.newBuilder()
        .setIdentifier(container.getId().toString())
        .setNodeId(container.getNodeId().toString())
        .setResourceMemory(container.getResource().getMemory())
        .build());
    updateRuntimeStatus();
  }

  /**
   * Handles container status reports. Calls come from YARN.
   *
   * @param value containing the container status
   */
  private void onContainerStatus(final ContainerStatus value) {

    final boolean hasContainer = this.containers.hasContainer(value.getContainerId().toString());

    if (hasContainer) {
      LOG.log(Level.FINE, "Received container status: {0}", value.getContainerId());

      final ResourceStatusProto.Builder status =
          ResourceStatusProto.newBuilder().setIdentifier(value.getContainerId().toString());

      switch (value.getState()) {
        case COMPLETE:
          LOG.info("container complete");
          // TODO: should we consider KILLED state? FYI: exit code = 143
          status.setState(value.getExitStatus() > 0 ? ReefServiceProtos.State.FAILED : ReefServiceProtos.State.DONE);
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

      this.reefEventHandlers.onResourceStatus(status.build());
    }
  }


  synchronized void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests) {
    synchronized (this.containers) {
      this.containerRequestCounter.incrementBy(containerRequests.length);
    }
    for (final AMRMClient.ContainerRequest containerRequest : containerRequests) {
      LOG.log(Level.FINEST, "Adding container request: " + containerRequest);
      this.resourceManager.addContainerRequest(containerRequest);
    }
    this.updateRuntimeStatus();
    LOG.log(Level.INFO, "Done adding container requests to YARN");
  }


  /**
   * Update the driver with my current status
   */

  private void updateRuntimeStatus() {
    final DriverRuntimeProtocol.RuntimeStatusProto.Builder builder;
    synchronized (this.containers) {
      builder = DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
          .setName(RUNTIME_NAME)
          .setState(ReefServiceProtos.State.RUNNING)
          .setOutstandingContainerRequests(this.containerRequestCounter.get());
      for (final String allocatedContainerId : this.containers.getContainerIds()) {
        builder.addContainerAllocation(allocatedContainerId);
      }
    }
    this.reefEventHandlers.onRuntimeStatus(builder.build());
  }

  private void onRuntimeError(final Throwable throwable) {
    // SHUTDOWN YARN
    try {
      this.reefEventHandlers.close();
      resourceManager.unregisterApplicationMaster(FinalApplicationStatus.FAILED, throwable.getMessage(), null);
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Error shutting down YARN application", e);
    } finally {
      resourceManager.stop();
    }

    final RuntimeStatusProto.Builder runtimeStatusBuilder = RuntimeStatusProto.newBuilder()
        .setState(ReefServiceProtos.State.FAILED)
        .setName(RUNTIME_NAME);


    final Encoder<Throwable> codec = new ObjectSerializableCodec<>();
    runtimeStatusBuilder.setError(ReefServiceProtos.RuntimeErrorProto.newBuilder()
        .setName(RUNTIME_NAME)
        .setMessage(throwable.getMessage())
        .setException(ByteString.copyFrom(codec.encode(throwable)))
        .build())
        .build();

    this.reefEventHandlers.onRuntimeStatus(runtimeStatusBuilder.build());
  }
}
