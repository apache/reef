/**
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.proto.DriverRuntimeProtocol;
import org.apache.reef.proto.DriverRuntimeProtocol.NodeDescriptorProto;
import org.apache.reef.proto.DriverRuntimeProtocol.ResourceAllocationProto;
import org.apache.reef.proto.DriverRuntimeProtocol.ResourceStatusProto;
import org.apache.reef.proto.DriverRuntimeProtocol.RuntimeStatusProto;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorManager;
import org.apache.reef.runtime.yarn.driver.parameters.YarnHeartbeatPeriod;
import org.apache.reef.runtime.yarn.util.YarnTypes;
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

  private static final String ADD_FLAG = "+";

  private static final String REMOVE_FLAG = "-";

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

  @Inject
  YarnContainerManager(
      final YarnConfiguration yarnConf,
      final @Parameter(YarnHeartbeatPeriod.class) int yarnRMHeartbeatPeriod,
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


    this.yarnClient.init(this.yarnConf);

    this.resourceManager = AMRMClientAsync.createAMRMClientAsync(yarnRMHeartbeatPeriod, this);
    this.nodeManager = new NMClientAsyncImpl(this);
    LOG.log(Level.FINEST, "Instantiated YarnContainerManager");
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
      handleNewContainer(container, false);
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
      this.nodeIdToRackName.put(nodeReport.getNodeId().toString(), nodeReport.getRackName());
      onNodeReport(nodeReport);
    }
  }

  @Override
  public final float getProgress() {
    return 0; // TODO: return actual values for progress
  }

  @Override
  public final void onError(final Throwable throwable) {
    onRuntimeError(throwable);
  }

  @Override
  public final void onContainerStarted(
      final ContainerId containerId, final Map<String, ByteBuffer> stringByteBufferMap) {
    final Optional<Container> container = this.containers.getOptional(containerId.toString());
    if (container.isPresent()) {
      this.nodeManager.getContainerStatusAsync(containerId, container.get().getNodeId());
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
  public final void onStartContainerError(
      final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public final void onGetContainerStatusError(
      final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
  }

  @Override
  public final void onStopContainerError(
      final ContainerId containerId, final Throwable throwable) {
    handleContainerError(containerId, throwable);
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
    logContainerRemoval(container.getId().toString());
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

    } catch (final YarnException | IOException e) {
      LOG.log(Level.WARNING, "Unable to register application master.", e);
      onRuntimeError(e);
    }

    // TODO: this is currently being developed on a hacked 2.4.0 bits, should be 2.4.1
    final String minVersionToGetPreviousContainer = "2.4.0";

    // when supported, obtain the list of the containers previously allocated, and write info to driver folder
    if (YarnTypes.isAtOrAfterVersion(minVersionToGetPreviousContainer)) {
      LOG.log(Level.FINEST, "Hadoop version is {0} or after with support to retain previous containers, processing previous containers.", minVersionToGetPreviousContainer);
      processPreviousContainers();
    }
  }

  void onStop() {

    LOG.log(Level.FINE, "Stop Runtime: RM status {0}", this.resourceManager.getServiceState());

    if (this.resourceManager.getServiceState() == Service.STATE.STARTED) {
      // invariant: if RM is still running then we declare success.
      try {
        this.reefEventHandlers.close();
        this.resourceManager.unregisterApplicationMaster(
            FinalApplicationStatus.SUCCEEDED, null, null);
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

  private void processPreviousContainers() {
    final List<Container> previousContainers = this.registration.getRegistration().getContainersFromPreviousAttempts();
    if (previousContainers != null && !previousContainers.isEmpty()) {
      LOG.log(Level.INFO, "Driver restarted, with {0} previous containers", previousContainers.size());
      this.driverStatusManager.setNumPreviousContainers(previousContainers.size());
      final Set<String> expectedContainers = getExpectedContainersFromLogReplay();
      final int numExpectedContainers = expectedContainers.size();
      final int numPreviousContainers = previousContainers.size();
      if (numExpectedContainers > numPreviousContainers) {
        // we expected more containers to be alive, some containers must have died during driver restart
        LOG.log(Level.WARNING, "Expected {0} containers while only {1} are still alive", new Object[]{numExpectedContainers, numPreviousContainers});
        final Set<String> previousContainersIds = new HashSet<>();
        for (final Container container : previousContainers) {
          previousContainersIds.add(container.getId().toString());
        }
        for (final String expectedContainerId : expectedContainers) {
          if (!previousContainersIds.contains(expectedContainerId)) {
            logContainerRemoval(expectedContainerId);
            LOG.log(Level.WARNING, "Expected container [{0}] not alive, must have failed during driver restart.", expectedContainerId);
            informAboutConatinerFailureDuringRestart(expectedContainerId);
          }
        }
      }
      if (numExpectedContainers < numPreviousContainers) {
        // somehow we have more alive evaluators, this should not happen
        throw new RuntimeException("Expected only [" + numExpectedContainers + "] containers but resource manager believe that [" + numPreviousContainers + "] are outstanding for driver.");
      }

      //  numExpectedContainers == numPreviousContainers
      for (final Container container : previousContainers) {
        LOG.log(Level.FINE, "Previous container: [{0}]", container.toString());
        if (!expectedContainers.contains(container.getId().toString())) {
          throw new RuntimeException("Not expecting container " + container.getId().toString());
        }
        handleNewContainer(container, true);
      }
    }
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

      final ResourceStatusProto.Builder status =
          ResourceStatusProto.newBuilder().setIdentifier(containerId);

      switch (value.getState()) {
        case COMPLETE:
          LOG.log(Level.FINE, "Container completed: status {0}", value.getExitStatus());
          switch (value.getExitStatus()) {
            case 0:
              status.setState(ReefServiceProtos.State.DONE);
              break;
            case 143:
              status.setState(ReefServiceProtos.State.KILLED);
              break;
            default:
              status.setState(ReefServiceProtos.State.FAILED);
          }
          status.setExitCode(value.getExitStatus());
          // remove the completed container (can be either done/killed/failed) from book keeping
          this.containers.removeAndGet(containerId);
          logContainerRemoval(containerId);
          break;
        default:
          LOG.info("Container running");
          status.setState(ReefServiceProtos.State.RUNNING);
      }

      if (value.getDiagnostics() != null) {
        LOG.log(Level.FINE, "Container diagnostics: {0}", value.getDiagnostics());
        status.setDiagnostics(value.getDiagnostics());
      }

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
  private void handleNewContainer(final Container container, final boolean isRecoveredContainer) {

    LOG.log(Level.FINE, "allocated container: id[ {0} ]", container.getId());
    // recovered container is not new allocation, it is just checking back from previous driver failover
    if (!isRecoveredContainer) {
      synchronized (this) {
        if (matchContainerWithPendingRequest(container)) {
          final AMRMClient.ContainerRequest matchedRequest = this.requestsAfterSentToRM.peek();
          this.containerRequestCounter.decrement();
          this.containers.add(container);

          LOG.log(Level.FINEST, "{0} matched with {1}", new Object[]{container.toString(), matchedRequest.toString()});

          // Due to the bug YARN-314 and the workings of AMRMCClientAsync, when x-priority m-capacity zero-container request
          // and x-priority n-capacity nonzero-container request are sent together, where m > n, RM ignores the latter.
          // Therefore it is necessary avoid sending zero-container request, even it means getting extra containers.
          // It is okay to send nonzero m-capacity and n-capacity request together since bigger containers can be matched.
          // TODO: revisit this when implementing locality-strictness (i.e. a specific rack request can be ignored)
          if (this.requestsAfterSentToRM.size() > 1) {
            try {
              this.resourceManager.removeContainerRequest(matchedRequest);
            } catch (final Exception e) {
              LOG.log(Level.WARNING, "Nothing to remove from Async AMRM client's queue, removal attempt failed with exception", e);
            }
          }

          this.requestsAfterSentToRM.remove();
          doHomogeneousRequests();

          LOG.log(Level.FINEST, "Allocated Container: memory = {0}, core number = {1}", new Object[]{container.getResource().getMemory(), container.getResource().getVirtualCores()});
          this.reefEventHandlers.onResourceAllocation(ResourceAllocationProto.newBuilder()
              .setIdentifier(container.getId().toString())
              .setNodeId(container.getNodeId().toString())
              .setResourceMemory(container.getResource().getMemory())
              .setVirtualCores(container.getResource().getVirtualCores())
              .build());
          // we only add this to Container log after the Container has been registered as an REEF Evaluator.
          logContainerAddition(container.getId().toString());
          this.updateRuntimeStatus();
        } else {
          LOG.log(Level.WARNING, "Got an extra container {0} that doesn't match, releasing...", container.getId());
          this.resourceManager.releaseAssignedContainer(container.getId());
        }
      }
    }
  }

  private synchronized void doHomogeneousRequests() {
    if (this.requestsAfterSentToRM.isEmpty()) {
      final AMRMClient.ContainerRequest firstRequest = this.requestsBeforeSentToRM.peek();

      while (!this.requestsBeforeSentToRM.isEmpty() && isSameKindOfRequest(firstRequest, this.requestsBeforeSentToRM.peek())) {
        final AMRMClient.ContainerRequest homogeneousRequest = this.requestsBeforeSentToRM.remove();
        this.resourceManager.addContainerRequest(homogeneousRequest);
        this.requestsAfterSentToRM.add(homogeneousRequest);
      }
    }
  }

  private boolean isSameKindOfRequest(AMRMClient.ContainerRequest r1, AMRMClient.ContainerRequest r2) {
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
  private boolean matchContainerWithPendingRequest(Container container) {
    if (this.requestsAfterSentToRM.isEmpty()) {
      return false;
    }

    final AMRMClient.ContainerRequest request = this.requestsAfterSentToRM.peek();
    final boolean resourceCondition = container.getResource().getMemory() >= request.getCapability().getMemory(); // TODO: check vcores once YARN-2380 is resolved
    final boolean nodeCondition = request.getNodes() == null
        || request.getNodes().contains(container.getNodeId().getHost());
    final boolean rackCondition = request.getRacks() == null
        || request.getRacks().contains(this.nodeIdToRackName.get(container.getNodeId().toString()));

    return resourceCondition && (request.getRelaxLocality() || (rackCondition && nodeCondition));
  }

  /**
   * Update the driver with my current status
   */
  private void updateRuntimeStatus() {

    final DriverRuntimeProtocol.RuntimeStatusProto.Builder builder =
        DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
            .setName(RUNTIME_NAME)
            .setState(ReefServiceProtos.State.RUNNING)
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

  private Set<String> getExpectedContainersFromLogReplay() {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    final Set<String> expectedContainers = new HashSet<>();
    try {
      final FileSystem fs = FileSystem.get(config);
      final Path path = new Path(getChangeLogLocation());
      if (!fs.exists(path)) {
        // empty set
        return expectedContainers;
      } else {
        final BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null) {
          if (line.startsWith(ADD_FLAG)) {
            final String containerId = line.substring(ADD_FLAG.length());
            if (expectedContainers.contains(containerId)) {
              throw new RuntimeException("Duplicated add container record found in the change log for container " + containerId);
            }
            expectedContainers.add(containerId);
          } else if (line.startsWith(REMOVE_FLAG)) {
            final String containerId = line.substring(REMOVE_FLAG.length());
            if (!expectedContainers.contains(containerId)) {
              throw new RuntimeException("Change log includes record that try to remove non-exist or duplicate remove record for container + " + containerId);
            }
            expectedContainers.remove(containerId);
          }
          line = br.readLine();
        }
        br.close();
      }
    } catch (final IOException e) {
      throw new RuntimeException("Cannot read from log file", e);
    }
    return expectedContainers;
  }

  private void informAboutConatinerFailureDuringRestart(final String containerId) {
    LOG.log(Level.WARNING, "Container [" + containerId +
        "] has failed during driver restart process, FailedEvaluaorHandler will be triggered, but no additional evaluator can be requested due to YARN-2433.");
    // trigger a failed evaluator event
    this.reefEventHandlers.onResourceStatus(ResourceStatusProto.newBuilder()
        .setIdentifier(containerId)
        .setState(ReefServiceProtos.State.FAILED)
        .setExitCode(1)
        .setDiagnostics("Container [" + containerId + "] failed during driver restart process.")
        .setIsFromPreviousDriver(true)
        .build());
  }

  private void writeToEvaluatorLog(final String entry) throws IOException {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    final FileSystem fs = getFileSystemInstance();
    final Path path = new Path(getChangeLogLocation());
    final boolean appendToLog = fs.exists(path);

    try (
        final BufferedWriter bw = appendToLog ?
            new BufferedWriter(new OutputStreamWriter(fs.append(path))) :
            new BufferedWriter(new OutputStreamWriter(fs.create(path)));
    ) {
      bw.write(entry);
    } catch (final IOException e) {
      if (appendToLog) {
        LOG.log(Level.FINE, "Unable to add an entry to the Evaluator log. Attempting append by delete and recreate", e);
        appendByDeleteAndCreate(fs, path, entry);
      }
    }
  }

  private FileSystem getFileSystemInstance() throws IOException {
    final org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
    config.setBoolean("dfs.support.append", true);
    config.setBoolean("dfs.support.broken.append", true);
    return FileSystem.get(config);
  }

  /**
   * For certain HDFS implementation, the append operation may not be supported (e.g., Azure blob - wasb)
   * in this case, we will emulate the append operation by reading the content, appending entry at the end,
   * then recreating the file with appended content.
   *
   * @throws java.io.IOException when the file can't be written.
   */

  private void appendByDeleteAndCreate(final FileSystem fs, final Path path, final String appendEntry) throws IOException {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    try (final InputStream inputStream = fs.open(path)) {
      IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    final String newContent = outputStream.toString() + appendEntry;
    fs.delete(path, true);

    try (final FSDataOutputStream newOutput = fs.create(path);
         final InputStream newInput = new ByteArrayInputStream(newContent.getBytes())) {
      IOUtils.copyBytes(newInput, newOutput, 4096, true);
    }

  }

  private String getChangeLogLocation() {
    return "/ReefApplications/" + EvaluatorManager.getJobIdentifier() + "/evaluatorsChangesLog";
  }

  private void logContainerAddition(final String containerId) {
    final String entry = ADD_FLAG + containerId + System.lineSeparator();
    try {
      writeToEvaluatorLog(entry);
    } catch (final IOException e) {
      LOG.log(Level.WARNING, "Unable to log the addition of container [" + containerId +
          "] to the container log. Driver restart won't work properly.", e);
    }
  }

  private void logContainerRemoval(final String containerId) {
    final String entry = REMOVE_FLAG + containerId + System.lineSeparator();
    try {
      writeToEvaluatorLog(entry);
    } catch (final IOException e) {
      LOG.log(Level.WARNING, "Unable to log the removal of container [" + containerId +
          "] to the container log. Driver restart won't work properly.", e);
    }
  }

}
