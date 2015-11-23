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
import org.apache.hadoop.yarn.client.api.YarnClient;
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
import org.apache.reef.wake.remote.Encoder;
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

  private final YarnClient yarnClient = YarnClient.createYarnClient();

  private final Queue<AMRMClient.ContainerRequest> requestsBeforeSentToRM = new ConcurrentLinkedQueue<>();

  private final Queue<AMRMClient.ContainerRequest> requestsAfterSentToRM = new ConcurrentLinkedQueue<>();

  private final Map<String, String> nodeIdToRackName = new ConcurrentHashMap<>();

  private final YarnConfiguration yarnConf;
  private final AMRMClientAsync resourceManager;
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
  YarnContainerManager(
      final YarnConfiguration yarnConf,
      @Parameter(YarnHeartbeatPeriod.class) final int yarnRMHeartbeatPeriod,
      final REEFEventHandlers reefEventHandlers,
      final Containers containers,
      final ApplicationMasterRegistration registration,
      final ContainerRequestCounter containerRequestCounter,
      final DriverStatusManager driverStatusManager,
      final REEFFileNames reefFileNames,
      @Parameter(JobSubmissionDirectory.class) final String jobSubmissionDirectory,
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


    this.yarnClient.init(this.yarnConf);

    this.resourceManager = AMRMClientAsync.createAMRMClientAsync(yarnRMHeartbeatPeriod, this);
    this.nodeManager = new NMClientAsyncImpl(this);
    this.jobSubmissionDirectory = jobSubmissionDirectory;
    this.reefFileNames = reefFileNames;
    this.progressProvider = progressProvider;
    LOG.log(Level.FINEST, "Instantiated YarnContainerManager");
  }


  @Override
  public void onContainersCompleted(final List<ContainerStatus> containerStatuses) {
    for (final ContainerStatus containerStatus : containerStatuses) {
      onContainerStatus(containerStatus);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public void onContainersAllocated(final List<Container> containers) {

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
    this.reefEventHandlers.onRuntimeStatus(RuntimeStatusEventImpl.newBuilder()
        .setName(RUNTIME_NAME).setState(State.DONE).build());
    this.driverStatusManager.onError(new Exception("Shutdown requested by YARN."));
  }

  @Override
  public void onNodesUpdated(final List<NodeReport> nodeReports) {
    for (final NodeReport nodeReport : nodeReports) {
      this.nodeIdToRackName.put(nodeReport.getNodeId().toString(), nodeReport.getRackName());
      onNodeReport(nodeReport);
    }
  }

  @Override
  public float getProgress() {
    try {
      return Math.max(Math.min(1, progressProvider.get().getProgress()), 0);
    } catch (final Exception e) {
      // An Exception must be caught and logged here because YARN swallows the Exception and fails the job.
      LOG.log(Level.WARNING, "An exception occurred in ProgressProvider.getProgress(), with message : " +
          e.getMessage() + ". Returning 0 as progress.");
      return 0f;
    }
  }

  @Override
  public void onError(final Throwable throwable) {
    onRuntimeError(throwable);
  }

  @Override
  public void onContainerStarted(
      final ContainerId containerId, final Map<String, ByteBuffer> stringByteBufferMap) {
    final Optional<Container> container = this.containers.getOptional(containerId.toString());
    if (container.isPresent()) {
      this.nodeManager.getContainerStatusAsync(containerId, container.get().getNodeId());
    }
  }

  @Override
  public void onContainerStatusReceived(
      final ContainerId containerId, final ContainerStatus containerStatus) {
    onContainerStatus(containerStatus);
  }

  @Override
  public void onContainerStopped(final ContainerId containerId) {
    final boolean hasContainer = this.containers.hasContainer(containerId.toString());
    if (hasContainer) {
      final ResourceStatusEventImpl.Builder resourceStatusBuilder =
          ResourceStatusEventImpl.newBuilder().setIdentifier(containerId.toString());
      resourceStatusBuilder.setState(State.DONE);
      this.reefEventHandlers.onResourceStatus(resourceStatusBuilder.build());
    }
  }

  @Override
  public void onStartContainerError(
      final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public void onGetContainerStatusError(
      final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public void onStopContainerError(
      final ContainerId containerId, final Throwable throwable) {
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

  void onStart() {

    this.yarnClient.start();
    this.resourceManager.init(this.yarnConf);
    this.resourceManager.start();
    this.nodeManager.init(this.yarnConf);
    this.nodeManager.start();

    try {
      for (final NodeReport nodeReport : this.yarnClient.getNodeReports(NodeState.RUNNING)) {
        onNodeReport(nodeReport);
      }
    } catch (IOException | YarnException e) {
      LOG.log(Level.WARNING, "Unable to fetch node reports from YARN.", e);
      onRuntimeError(e);
    }

    try {
      this.registration.setRegistration(this.resourceManager.registerApplicationMaster(
          "", 0, this.trackingURLProvider.getTrackingUrl()));
      LOG.log(Level.FINE, "YARN registration: {0}", registration);
      final FileSystem fs = FileSystem.get(this.yarnConf);
      final Path outputFileName = new Path(this.jobSubmissionDirectory, this.reefFileNames.getDriverHttpEndpoint());
      final FSDataOutputStream out = fs.create(outputFileName);
      out.writeBytes(this.trackingURLProvider.getTrackingUrl() + "\n");
      out.flush();
      out.close();
    } catch (final YarnException | IOException e) {
      LOG.log(Level.WARNING, "Unable to register application master.", e);
      onRuntimeError(e);
    }
  }

  void onStop(final Throwable exception) {

    LOG.log(Level.FINE, "Stop Runtime: RM status {0}", this.resourceManager.getServiceState());

    if (this.resourceManager.getServiceState() == Service.STATE.STARTED) {
      // invariant: if RM is still running then we declare success.
      try {
        this.reefEventHandlers.close();
        if (exception == null) {
          this.resourceManager.unregisterApplicationMaster(
              FinalApplicationStatus.SUCCEEDED, null, null);
        } else {
          // Note: We don't allow RM to restart our applications if it's an application level failure.
          // If applications are to be long-running, they should catch Exceptions before the REEF level
          // instead of relying on the RM restart mechanism.
          // For this case, we make a strong assumption that REEF does not allow its own unhandled Exceptions
          // to leak to this stage.
          final String failureMsg = String.format("Application failed due to:%n%s%n" +
              "With stack trace:%n%s", exception.getMessage(), ExceptionUtils.getStackTrace(exception));
          this.resourceManager.unregisterApplicationMaster(
              FinalApplicationStatus.FAILED, failureMsg, null);
        }

        this.resourceManager.close();
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Error shutting down YARN application", e);
      }
    }

    if (this.nodeManager.getServiceState() == Service.STATE.STARTED) {
      try {
        this.nodeManager.close();
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

    final ResourceStatusEventImpl.Builder resourceStatusBuilder =
        ResourceStatusEventImpl.newBuilder().setIdentifier(containerId.toString());

    resourceStatusBuilder.setState(State.FAILED);
    resourceStatusBuilder.setExitCode(1);
    resourceStatusBuilder.setDiagnostics(throwable.getMessage());
    this.reefEventHandlers.onResourceStatus(resourceStatusBuilder.build());
  }

  /**
   * Handles container status reports. Calls come from YARN.
   *
   * @param value containing the container status
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

      // The ResourceStatusHandler should close and release the Evaluator for us if the state is a terminal state.
      this.reefEventHandlers.onResourceStatus(status.build());
    }
  }

  void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests) {

    synchronized (this) {
      this.containerRequestCounter.incrementBy(containerRequests.length);
      this.requestsBeforeSentToRM.addAll(Arrays.asList(containerRequests));
      doHomogeneousRequests();
    }

    this.updateRuntimeStatus();
  }

  /**
   * Handles new container allocations. Calls come from YARN.
   *
   * @param container newly allocated
   */
  private void handleNewContainer(final Container container) {

    LOG.log(Level.FINE, "allocated container: id[ {0} ]", container.getId());
    synchronized (this) {
      if (matchContainerWithPendingRequest(container)) {
        final AMRMClient.ContainerRequest matchedRequest = this.requestsAfterSentToRM.peek();
        this.containerRequestCounter.decrement();
        this.containers.add(container);

        LOG.log(Level.FINEST, "{0} matched with {1}", new Object[]{container.toString(), matchedRequest.toString()});

        // Due to the bug YARN-314 and the workings of AMRMCClientAsync, when x-priority m-capacity zero-container
        // request and x-priority n-capacity nonzero-container request are sent together, where m > n, RM ignores
        // the latter.
        // Therefore it is necessary avoid sending zero-container request, even it means getting extra containers.
        // It is okay to send nonzero m-capacity and n-capacity request together since bigger containers
        // can be matched.
        // TODO[JIRA REEF-42, REEF-942]: revisit this when implementing locality-strictness
        // (i.e. a specific rack request can be ignored)
        if (this.requestsAfterSentToRM.size() > 1) {
          try {
            this.resourceManager.removeContainerRequest(matchedRequest);
          } catch (final Exception e) {
            LOG.log(Level.WARNING, "Nothing to remove from Async AMRM client's queue, " +
                "removal attempt failed with exception", e);
          }
        }

        this.requestsAfterSentToRM.remove();
        doHomogeneousRequests();

        LOG.log(Level.FINEST, "Allocated Container: memory = {0}, core number = {1}",
            new Object[]{container.getResource().getMemory(), container.getResource().getVirtualCores()});
        this.reefEventHandlers.onResourceAllocation(ResourceEventImpl.newAllocationBuilder()
            .setIdentifier(container.getId().toString())
            .setNodeId(container.getNodeId().toString())
            .setResourceMemory(container.getResource().getMemory())
            .setVirtualCores(container.getResource().getVirtualCores())
            .setRackName(rackNameFormatter.getRackName(container))
            .setRuntimeName(RuntimeIdentifier.RUNTIME_NAME)
            .build());
        this.updateRuntimeStatus();
      } else {
        LOG.log(Level.WARNING, "Got an extra container {0} that doesn't match, releasing...", container.getId());
        this.resourceManager.releaseAssignedContainer(container.getId());
      }
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

    final RuntimeStatusEventImpl.Builder builder =
        RuntimeStatusEventImpl.newBuilder()
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
      this.resourceManager.unregisterApplicationMaster(
          FinalApplicationStatus.FAILED, throwable.getMessage(), null);
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Error shutting down YARN application", e);
    } finally {
      this.resourceManager.stop();
    }

    final RuntimeStatusEventImpl.Builder runtimeStatusBuilder = RuntimeStatusEventImpl.newBuilder()
        .setState(State.FAILED)
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
