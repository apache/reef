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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.reef.runtime.common.files.ClasspathProvider;
import com.microsoft.reef.runtime.common.files.REEFFileNames;
import com.microsoft.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.LaunchCommandBuilder;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.runtime.local.client.parameters.DefaultNumberOfCores;
import com.microsoft.reef.runtime.local.client.parameters.DefaultMemorySize;
import com.microsoft.reef.runtime.local.driver.parameters.GlobalFiles;
import com.microsoft.reef.runtime.local.driver.parameters.GlobalLibraries;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A resource manager that uses threads to execute containers.
 */
@Private
@DriverSide
public final class ResourceManager {

  private final static Logger LOG = Logger.getLogger(ResourceManager.class.getName());

  private final ResourceRequestQueue requestQueue = new ResourceRequestQueue();

  private final EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> allocationHandler;
  private final ContainerManager theContainers;
  private final EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusHandlerEventHandler;
  private final int defaultMemorySize;
  private final int defaultNumberOfCores;
  private final ConfigurationSerializer configurationSerializer;
  private final RemoteManager remoteManager;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpathProvider;
  private final double jvmHeapFactor;

  @Inject
  ResourceManager(
      final ContainerManager containerManager,
      final @Parameter(RuntimeParameters.ResourceAllocationHandler.class) EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> allocationHandler,
      final @Parameter(RuntimeParameters.RuntimeStatusHandler.class) EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusHandlerEventHandler,
      final @Parameter(GlobalLibraries.class) Set<String> globalLibraries,
      final @Parameter(GlobalFiles.class) Set<String> globalFiles,
      final @Parameter(DefaultMemorySize.class) int defaultMemorySize,
      final @Parameter(DefaultNumberOfCores.class) int defaultNumberOfCores,
      final @Parameter(JVMHeapSlack.class) double jvmHeapSlack,
      final ConfigurationSerializer configurationSerializer,
      final RemoteManager remoteManager,
      final REEFFileNames fileNames,
      final ClasspathProvider classpathProvider) {

    this.theContainers = containerManager;
    this.allocationHandler = allocationHandler;
    this.runtimeStatusHandlerEventHandler = runtimeStatusHandlerEventHandler;
    this.configurationSerializer = configurationSerializer;
    this.remoteManager = remoteManager;
    this.defaultMemorySize = defaultMemorySize;
    this.defaultNumberOfCores = defaultNumberOfCores;
    this.fileNames = fileNames;
    this.classpathProvider = classpathProvider;
    this.jvmHeapFactor = 1.0 - jvmHeapSlack;

    LOG.log(Level.FINE, "Instantiated 'ResourceManager'");
  }

  /**
   * Receives a resource request.
   * <p/>
   * If the request can be met, it will also be satisfied immediately.
   *
   * @param resourceRequest the resource request to be handled.
   */
  final void onResourceRequest(final DriverRuntimeProtocol.ResourceRequestProto resourceRequest) {
    synchronized (this.theContainers) {
      this.requestQueue.add(new ResourceRequest(resourceRequest));
      this.checkRequestQueue();
    }
  }

  /**
   * Receives and processes a resource release request.
   *
   * @param releaseRequest the release request to be processed
   */
  final void onResourceReleaseRequest(final DriverRuntimeProtocol.ResourceReleaseProto releaseRequest) {
    synchronized (this.theContainers) {
      LOG.log(Level.FINEST, "Release container: {0}", releaseRequest.getIdentifier());
      this.theContainers.release(releaseRequest.getIdentifier());
      this.checkRequestQueue();
    }
  }

  /**
   * Called when the ReefRunnableProcessObserver detects that the Evaluator process has exited.
   *
   * @param evaluatorId the ID of the Evaluator that exited.
   */
  public final void onEvaluatorExit(final String evaluatorId) {
    synchronized (this.theContainers) {
      this.theContainers.release(evaluatorId);
      this.checkRequestQueue();
    }
  }

  /**
   * Processes a resource launch request.
   *
   * @param launchRequest the launch request to be processed.
   */
  final void onResourceLaunchRequest(
      final DriverRuntimeProtocol.ResourceLaunchProto launchRequest) {

    synchronized (this.theContainers) {

      final Container c = this.theContainers.get(launchRequest.getIdentifier());

      // Add the global files and libraries.
      c.addGlobalFiles(this.fileNames.getGlobalFolder());
      c.addLocalFiles(getLocalFiles(launchRequest));

      // Make the configuration file of the evaluator.
      final File evaluatorConfigurationFile = new File(c.getFolder(), fileNames.getEvaluatorConfigurationPath());

      try {
        this.configurationSerializer.toFile(this.configurationSerializer.fromString(launchRequest.getEvaluatorConf()),
            evaluatorConfigurationFile);
      } catch (final IOException | BindException e) {
        throw new RuntimeException("Unable to write configuration.", e);
      }

      // Assemble the command line
      final LaunchCommandBuilder commandBuilder;
      switch (launchRequest.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder()
              .setClassPath(this.classpathProvider.getEvaluatorClasspath());
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported container type: " + launchRequest.getType());
      }

      final List<String> command = commandBuilder
          .setErrorHandlerRID(this.remoteManager.getMyIdentifier())
          .setLaunchID(c.getNodeID())
          .setConfigurationFileName(this.fileNames.getEvaluatorConfigurationPath())
          .setMemory((int) (this.jvmHeapFactor * c.getMemory()))
          .build();

      LOG.log(Level.FINEST, "Launching container: {0}", c);
      c.run(command);
    }
  }

  /**
   * Checks the allocation queue for new allocations and if there are any
   * satisfies them.
   */
  private void checkRequestQueue() {

    if (this.theContainers.hasContainerAvailable() && this.requestQueue.hasOutStandingRequests()) {

      // Record the satisfaction of one request and get its details.
      final DriverRuntimeProtocol.ResourceRequestProto requestProto = this.requestQueue.satisfyOne();

      // Allocate a Container
      final Container container = this.theContainers.allocateOne(
          requestProto.hasMemorySize() ? requestProto.getMemorySize() : this.defaultMemorySize,
          requestProto.hasVirtualCores() ? requestProto.getVirtualCores() : this.defaultNumberOfCores);

      // Tell the receivers about it
      final DriverRuntimeProtocol.ResourceAllocationProto alloc =
          DriverRuntimeProtocol.ResourceAllocationProto.newBuilder()
              .setIdentifier(container.getContainerID())
              .setNodeId(container.getNodeID())
              .setResourceMemory(container.getMemory())
              .setVirtualCores(container.getNumberOfCores())
              .build();

      LOG.log(Level.FINEST, "Allocating container: {0}", container);
      this.allocationHandler.onNext(alloc);

      // update REEF
      this.sendRuntimeStatus();

      // Check whether we can satisfy another one.
      this.checkRequestQueue();

    } else {
      this.sendRuntimeStatus();
    }
  }

  private void sendRuntimeStatus() {

    final DriverRuntimeProtocol.RuntimeStatusProto msg =
        DriverRuntimeProtocol.RuntimeStatusProto.newBuilder()
            .setName("LOCAL")
            .setState(ReefServiceProtos.State.RUNNING)
            .setOutstandingContainerRequests(this.requestQueue.getNumberOfOutstandingRequests())
            .addAllContainerAllocation(this.theContainers.getAllocatedContainerIDs())
            .build();

    LOG.log(Level.INFO, "Allocated: {0}, Outstanding requests: {1}",
        new Object[]{msg.getContainerAllocationCount(), msg.getOutstandingContainerRequests()});
    this.runtimeStatusHandlerEventHandler.onNext(msg);
  }

  /**
   * Extracts the files out of the launchRequest.
   *
   * @param launchRequest the ResourceLaunchProto to parse
   * @return a list of files set in the given ResourceLaunchProto
   */
  private static List<File> getLocalFiles(final DriverRuntimeProtocol.ResourceLaunchProto launchRequest) {
    final List<File> files = new ArrayList<>();  // Libraries local to this evaluator
    for (final ReefServiceProtos.FileResourceProto frp : launchRequest.getFileList()) {
      files.add(new File(frp.getPath()).getAbsoluteFile());
    }
    return files;
  }
}
