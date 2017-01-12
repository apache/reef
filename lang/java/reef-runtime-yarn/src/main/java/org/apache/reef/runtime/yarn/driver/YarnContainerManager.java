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
package org.apache.reef.runtime.yarn.driver;

import com.google.protobuf.ByteString;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.driver.ProgressProvider;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEventImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEventImpl;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectory;
import org.apache.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

final class YarnContainerManager
    implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

  private static final Logger LOG = Logger.getLogger(YarnContainerManager.class.getName());

  private static final String RUNTIME_NAME = "YARN";

  /** Default hostname to provide in the Application Master registration. */
  private static final String AM_REGISTRATION_HOST = "";

  /** Default port number to provide in the Application Master registration. */
  private static final int AM_REGISTRATION_PORT = -1;

  private final Queue<AMRMClient.ContainerRequest> requestsBeforeSentToRM = new ConcurrentLinkedQueue<>();
  private final Queue<AMRMClient.ContainerRequest> requestsAfterSentToRM = new ConcurrentLinkedQueue<>();
  private final Map<String, String> nodeIdToRackName = new ConcurrentHashMap<>();

  private final YarnConfiguration yarnConf;
  private final AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;
  private final NMClientAsync nodeManager;
  private final REEFEventHandlers reefEventHandlers;
  private final Containers containers;
  private final ApplicationMasterRegistration registration;
  private final ContainerRequestCounter containerRequestCounter;
  private final DriverStatusManager driverStatusManager;
  private final TrackingURLProvider trackingURLProvider;
  private final String jobSubmissionDirectory;
  private final REEFFileNames reefFileNames;
  private final RackNameFormatter rackNameFormatter;
  private final InjectionFuture<ProgressProvider> progressProvider;

  @Inject
  private YarnContainerManager(
      @Parameter(YarnHeartbeatPeriod.class) final int yarnRMHeartbeatPeriod,
      @Parameter(JobSubmissionDirectory.class) final String jobSubmissionDirectory,
      final YarnConfiguration yarnConf,
      final REEFEventHandlers reefEventHandlers,
      final Containers containers,
      final ApplicationMasterRegistration registration,
      final ContainerRequestCounter containerRequestCounter,
      final DriverStatusManager driverStatusManager,
      final REEFFileNames reefFileNames,
      final TrackingURLProvider trackingURLProvider,
      final RackNameFormatter rackNameFormatter,
      final InjectionFuture<ProgressProvider> progressProvider) throws IOException {

    this.reefEventHandlers = reefEventHandlers;
    this.driverStatusManager = driverStatusManager;

    this.containers = containers;
    this.registration = registration;
    this.containerRequestCounter = containerRequestCounter;
    this.yarnConf = yarnConf;
    this.trackingURLProvider = trackingURLProvider;
    this.rackNameFormatter = rackNameFormatter;

    this.resourceManager = AMRMClientAsync.createAMRMClientAsync(yarnRMHeartbeatPeriod, this);
    this.nodeManager = new NMClientAsyncImpl(this);

    this.jobSubmissionDirectory = jobSubmissionDirectory;
    this.reefFileNames = reefFileNames;
    this.progressProvider = progressProvider;

    LOG.log(Level.FINEST, "Instantiated YarnContainerManager: {0}", this.registration);
  }

  /**
   * RM Callback: RM reports some completed containers. Update status of each container in the list.
   * @param completedContainers list of completed containers.
   */
  @Override
  public void onContainersCompleted(final List<ContainerStatus> completedContainers) {
    for (final ContainerStatus containerStatus : completedContainers) {
      this.onContainerStatus(containerStatus);
    }
  }

  /**
   * RM Callback: RM reports that some containers have been allocated.
   * @param allocatedContainers list of containers newly allocated by RM.
   */
  @Override
  public void onContainersAllocated(final List<Container> allocatedContainers) {

    String id = null; // ID is used for logging only

    if (LOG.isLoggable(Level.FINE)) {

      id = String.format("%s:%d", Thread.currentThread().getName().replace(' ', '_'), System.currentTimeMillis());

      LOG.log(Level.FINE, "TIME: Allocated Containers {0} {1} of {2}",
          new Object[] {id, allocatedContainers.size(), this.containerRequestCounter.get()});
    }

    for (final Container container : allocatedContainers) {
      this.handleNewContainer(container);
    }

    LOG.log(Level.FINE, "TIME: Processed Containers {0}", id);
  }

  /**
   * RM Callback: RM requests application shutdown.
   */
  @Override
  public void onShutdownRequest() {
    this.reefEventHandlers.onRuntimeStatus(RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME).setState(State.DONE).build());
    this.driverStatusManager.onError(new Exception("Shutdown requested by YARN."));
  }

  /**
   * RM Callback: RM reports status change of some nodes.
   * @param nodeReports list of nodes with changed status.
   */
  @Override
  public void onNodesUpdated(final List<NodeReport> nodeReports) {
    for (final NodeReport nodeReport : nodeReports) {
      this.nodeIdToRackName.put(nodeReport.getNodeId().toString(), nodeReport.getRackName());
      this.onNodeReport(nodeReport);
    }
  }

  /**
   * RM Callback: Report application progress to RM.
   * Progress is a floating point number between 0 and 1.
   * @return a floating point number between 0 and 1.
   */
  @Override
  public float getProgress() {
    try {
      return Math.max(Math.min(1, progressProvider.get().getProgress()), 0);
    } catch (final Exception e) {
      // An Exception must be caught and logged here because YARN swallows the Exception and fails the job.
      LOG.log(Level.WARNING, "Cannot get the application progress. Will return 0.", e);
      return 0;
    }
  }

  /**
   * RM Callback: RM reports an error.
   * @param throwable An exception thrown from RM.
   */
  @Override
  public void onError(final Throwable throwable) {
    this.onRuntimeError(throwable);
  }

  /**
   * NM Callback: NM accepts the starting container request.
   * @param containerId ID of a new container being started.
   * @param stringByteBufferMap a Map between the auxiliary service names and their outputs. Not used.
   */
  @Override
  public void onContainerStarted(final ContainerId containerId, final Map<String, ByteBuffer> stringByteBufferMap) {
    final Optional<Container> container = this.containers.getOptional(containerId.toString());
    if (container.isPresent()) {
      this.nodeManager.getContainerStatusAsync(containerId, container.get().getNodeId());
    }
  }

  /**
   * NM Callback: NM reports container status.
   * @param containerId ID of a container with the status being reported.
   * @param containerStatus YARN container status.
   */
  @Override
  public void onContainerStatusReceived(final ContainerId containerId, final ContainerStatus containerStatus) {
    onContainerStatus(containerStatus);
  }

  /**
   * NM Callback: NM reports stop of a container.
   * @param containerId ID of a container stopped.
   */
  @Override
  public void onContainerStopped(final ContainerId containerId) {
    final boolean hasContainer = this.containers.hasContainer(containerId.toString());
    if (hasContainer) {
      this.reefEventHandlers.onResourceStatus(
          ResourceStatusEventImpl.newBuilder()
             .setIdentifier(containerId.toString())
             .setState(State.DONE)
             .build());
    }
  }

  /**
   * NM Callback: NM reports failure on container start.
   * @param containerId ID of a container that has failed to start.
   * @param throwable An error that caused container to fail.
   */
  @Override
  public void onStartContainerError(final ContainerId containerId, final Throwable throwable) {
    this.handleContainerError(containerId, throwable);
  }

  /**
   * NM Callback: NM can not obtain status of the container.
   * @param containerId ID of a container that failed to report its status.
   * @param throwable An error that occured when querying status of a container.
   */
  @Override
  public void onGetContainerStatusError(final ContainerId containerId, final Throwable throwable) {
    this.handleContainerError(containerId, throwable);
  }

  /**
   * NM Callback: NM fails to stop the container.
   * @param containerId ID of the container that failed to stop.
   * @param throwable An error that occurred when trying to stop the container.
   */
  @Override
  public void onStopContainerError(final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  /**
   * Called by {@link YarnDriverRuntimeRestartManager} to record recovered containers
   * such that containers can be released properly on unrecoverable containers.
   */
  public void onContainersRecovered(final Set<Container> recoveredContainers) {
    for (final Container container : recoveredContainers) {
      containers.add(container);
    }
  }

  /**
   * Submit the given launchContext to the given container.
   */
  void submit(final Container container, final ContainerLaunchContext launchContext) {
    this.nodeManager.startContainerAsync(container, launchContext);
  }

  /**
   * Release the given container.
   */
  void release(final String containerId) {
    LOG.log(Level.FINE, "Release container: {0}", containerId);
    final Container container = this.containers.removeAndGet(containerId);
    this.resourceManager.releaseAssignedContainer(container.getId());
    updateRuntimeStatus();
  }

  /**
   * Start the YARN container manager.
   * This method is called from DriverRuntimeStartHandler via YARNRuntimeStartHandler.
   */
  void onStart() {

    LOG.log(Level.FINEST, "YARN registration: begin");

    this.resourceManager.init(this.yarnConf);
    this.resourceManager.start();

    this.nodeManager.init(this.yarnConf);
    this.nodeManager.start();

    LOG.log(Level.FINEST, "YARN registration: registered with RM and NM");

    try {

      this.registration.setRegistration(this.resourceManager.registerApplicationMaster(
          AM_REGISTRATION_HOST, AM_REGISTRATION_PORT, this.trackingURLProvider.getTrackingUrl()));

      LOG.log(Level.FINE, "YARN registration: AM registered: {0}", this.registration);

      final FileSystem fs = FileSystem.get(this.yarnConf);
      final Path outputFileName = new Path(this.jobSubmissionDirectory, this.reefFileNames.getDriverHttpEndpoint());

      try (final FSDataOutputStream out = fs.create(outputFileName)) {
        out.writeBytes(this.trackingURLProvider.getTrackingUrl() + '\n');
      }
    } catch (final YarnException | IOException e) {
      LOG.log(Level.WARNING, "Unable to register application master.", e);
      onRuntimeError(e);
    }

    LOG.log(Level.FINEST, "YARN registration: done: {0}", this.registration);
  }

  /**
   * Shut down YARN container manager.
   * This method is called from DriverRuntimeStopHandler via YARNRuntimeStopHandler.
   * @param exception Exception that caused driver to stop. Can be null if there was no error.
   */
  void onStop(final Throwable exception) {

    LOG.log(Level.FINE, "Stop Runtime: RM status {0}", this.resourceManager.getServiceState());

    if (this.resourceManager.getServiceState() == Service.STATE.STARTED) {

      // invariant: if RM is still running then we declare success.
      try {

        this.reefEventHandlers.close();

        if (exception == null) {
          this.resourceManager.unregisterApplicationMaster(
              FinalApplicationStatus.SUCCEEDED, "Success!", this.trackingURLProvider.getTrackingUrl());
        } else {

          // Note: We don't allow RM to restart our applications if it's an application level failure.
          // If applications are to be long-running, they should catch Exceptions before the REEF level
          // instead of relying on the RM restart mechanism.
          // For this case, we make a strong assumption that REEF does not allow its own unhandled Exceptions
          // to leak to this stage.
          final String failureMsg = String.format("Application failed due to:%n%s%n" +
              "With stack trace:%n%s", exception.getMessage(), ExceptionUtils.getStackTrace(exception));

          this.resourceManager.unregisterApplicationMaster(
              FinalApplicationStatus.FAILED, failureMsg, this.trackingURLProvider.getTrackingUrl());
        }

        this.resourceManager.close();
        LOG.log(Level.FINEST, "Container ResourceManager stopped successfully");

      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Error shutting down YARN application", e);
      }
    }

    if (this.nodeManager.getServiceState() == Service.STATE.STARTED) {
      try {
        this.nodeManager.close();
        LOG.log(Level.FINEST, "Container NodeManager stopped successfully");
      } catch (final IOException e) {
        LOG.log(Level.WARNING, "Error closing YARN Node Manager", e);
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // HELPER METHODS

  private void onNodeReport(final NodeReport nodeReport) {

    LOG.log(Level.FINE, "Send node descriptor: {0}", nodeReport);

    this.reefEventHandlers.onNodeDescriptor(NodeDescriptorEventImpl.newBuilder()
        .setIdentifier(nodeReport.getNodeId().toString())
        .setHostName(nodeReport.getNodeId().getHost())
        .setPort(nodeReport.getNodeId().getPort())
        .setMemorySize(nodeReport.getCapability().getMemory())
        .setRackName(nodeReport.getRackName())
        .build());
  }

  private void handleContainerError(final ContainerId containerId, final Throwable throwable) {

    this.reefEventHandlers.onResourceStatus(ResourceStatusEventImpl.newBuilder()
        .setIdentifier(containerId.toString())
        .setState(State.FAILED)
        .setExitCode(1)
        .setDiagnostics(throwable.getMessage())
        .build());
  }

  /**
   * Handles container status reports. Calls come from YARN.
   * @param value containing the container status.
   */
  private void onContainerStatus(final ContainerStatus value) {

    final String containerId = value.getContainerId().toString();
    final boolean hasContainer = this.containers.hasContainer(containerId);

    if (hasContainer) {
      LOG.log(Level.FINE, "Received container status: {0}", containerId);

      final ResourceStatusEventImpl.Builder status =
          ResourceStatusEventImpl.newBuilder().setIdentifier(containerId);

      switch (value.getState()) {
      case COMPLETE:
        LOG.log(Level.FINE, "Container completed: status {0}", value.getExitStatus());
        switch (value.getExitStatus()) {
        case 0:
          status.setState(State.DONE);
          break;
        case 143:
          status.setState(State.KILLED);
          break;
        default:
          status.setState(State.FAILED);
        }
        status.setExitCode(value.getExitStatus());
        break;
      default:
        LOG.info("Container running");
        status.setState(State.RUNNING);
      }

      if (value.getDiagnostics() != null) {
        LOG.log(Level.FINE, "Container diagnostics: {0}", value.getDiagnostics());
        status.setDiagnostics(value.getDiagnostics());
      }

      // ResourceStatusHandler should close and release the Evaluator for us if the state is a terminal state.
      this.reefEventHandlers.onResourceStatus(status.build());
    }
  }

  void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests) {

    synchronized (this) {
      this.containerRequestCounter.incrementBy(containerRequests.length);
      this.requestsBeforeSentToRM.addAll(Arrays.asList(containerRequests));
      this.doHomogeneousRequests();
    }

    this.updateRuntimeStatus();
  }

  /**
   * Handles new container allocations. Calls come from YARN.
   * @param container newly allocated YARN container.
   */
  private void handleNewContainer(final Container container) {

    LOG.log(Level.FINE, "allocated container: id[ {0} ]", container.getId());

    synchronized (this) {

      if (!matchContainerWithPendingRequest(container)) {
        LOG.log(Level.WARNING, "Got an extra container {0} that doesn't match, releasing...", container.getId());
        this.resourceManager.releaseAssignedContainer(container.getId());
        return;
      }

      final AMRMClient.ContainerRequest matchedRequest = this.requestsAfterSentToRM.peek();

      this.containerRequestCounter.decrement();
      this.containers.add(container);

      LOG.log(Level.FINEST, "{0} matched with {1}", new Object[] {container, matchedRequest});

      // Due to the bug YARN-314 and the workings of AMRMCClientAsync, when x-priority m-capacity zero-container
      // request and x-priority n-capacity nonzero-container request are sent together, where m > n, RM ignores
      // the latter.
      // Therefore it is necessary avoid sending zero-container request, even if it means getting extra containers.
      // It is okay to send nonzero m-capacity and n-capacity request together since bigger containers
      // can be matched.
      // TODO[JIRA REEF-42, REEF-942]: revisit this when implementing locality-strictness.
      // (i.e. a specific rack request can be ignored)
      if (this.requestsAfterSentToRM.size() > 1) {
        try {
          this.resourceManager.removeContainerRequest(matchedRequest);
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "Error removing request from Async AMRM client queue: " + matchedRequest, e);
        }
      }

      this.requestsAfterSentToRM.remove();
      this.doHomogeneousRequests();

      LOG.log(Level.FINEST, "Allocated Container: memory = {0}, core number = {1}",
          new Object[] {container.getResource().getMemory(), container.getResource().getVirtualCores()});

      this.reefEventHandlers.onResourceAllocation(ResourceEventImpl.newAllocationBuilder()
          .setIdentifier(container.getId().toString())
          .setNodeId(container.getNodeId().toString())
          .setResourceMemory(container.getResource().getMemory())
          .setVirtualCores(container.getResource().getVirtualCores())
          .setRackName(rackNameFormatter.getRackName(container))
          .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
          .build());

      this.updateRuntimeStatus();
    }
  }

  private synchronized void doHomogeneousRequests() {
    if (this.requestsAfterSentToRM.isEmpty()) {
      final AMRMClient.ContainerRequest firstRequest = this.requestsBeforeSentToRM.peek();

      while (!this.requestsBeforeSentToRM.isEmpty() &&
             isSameKindOfRequest(firstRequest, this.requestsBeforeSentToRM.peek())) {
        final AMRMClient.ContainerRequest homogeneousRequest = this.requestsBeforeSentToRM.remove();
        this.resourceManager.addContainerRequest(homogeneousRequest);
        this.requestsAfterSentToRM.add(homogeneousRequest);
      }
    }
  }

  private boolean isSameKindOfRequest(final AMRMClient.ContainerRequest r1, final AMRMClient.ContainerRequest r2) {
    return r1.getPriority().compareTo(r2.getPriority()) == 0
        && r1.getCapability().compareTo(r2.getCapability()) == 0
        && r1.getRelaxLocality() == r2.getRelaxLocality()
        && ListUtils.isEqualList(r1.getNodes(), r2.getNodes())
        && ListUtils.isEqualList(r1.getRacks(), r2.getRacks());
  }

  /**
   * Match to see whether the container satisfies the request.
   * We take into consideration that RM has some freedom in rounding
   * up the allocation and in placing containers on other machines.
   */
  private boolean matchContainerWithPendingRequest(final Container container) {

    if (this.requestsAfterSentToRM.isEmpty()) {
      return false;
    }

    final AMRMClient.ContainerRequest request = this.requestsAfterSentToRM.peek();

    final boolean resourceCondition = container.getResource().getMemory() >= request.getCapability().getMemory();

    // TODO[JIRA REEF-35]: check vcores once YARN-2380 is resolved
    final boolean nodeCondition = request.getNodes() == null
        || request.getNodes().contains(container.getNodeId().getHost());

    final boolean rackCondition = request.getRacks() == null
        || request.getRacks().contains(this.nodeIdToRackName.get(container.getNodeId().toString()));

    return resourceCondition && (request.getRelaxLocality() || rackCondition && nodeCondition);
  }

  /**
   * Update the driver with my current status.
   */
  private void updateRuntimeStatus() {

    final RuntimeStatusEventImpl.Builder builder = RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME)
        .setState(State.RUNNING)
        .setOutstandingContainerRequests(this.containerRequestCounter.get());

    for (final String allocatedContainerId : this.containers.getContainerIds()) {
      builder.addContainerAllocation(allocatedContainerId);
    }

    this.reefEventHandlers.onRuntimeStatus(builder.build());
  }

  private void onRuntimeError(final Throwable throwable) {

    // SHUTDOWN YARN
    try {
      this.reefEventHandlers.close();
      this.resourceManager.unregisterApplicationMaster(FinalApplicationStatus.FAILED, throwable.getMessage(), null);
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Error shutting down YARN application", e);
    } finally {
      this.resourceManager.stop();
    }

    final ReefServiceProtos.RuntimeErrorProto runtimeError =
        ReefServiceProtos.RuntimeErrorProto.newBuilder()
            .setName(RUNTIME_NAME)
            .setMessage(throwable.getMessage())
            .setException(ByteString.copyFrom(new ObjectSerializableCodec<>().encode(throwable)))
            .build();

    this.reefEventHandlers.onRuntimeStatus(
        RuntimeStatusEventImpl.newBuilder()
            .setState(State.FAILED)
            .setName(RUNTIME_NAME)
            .setError(runtimeError)
            .build());
  }
}
