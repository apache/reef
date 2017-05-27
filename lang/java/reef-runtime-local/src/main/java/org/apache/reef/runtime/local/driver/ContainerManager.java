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
package org.apache.reef.runtime.local.driver;

import org.apache.commons.lang.Validate;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.api.ResourceRequestEvent;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.Constants;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.local.client.parameters.DefaultMemorySize;
import org.apache.reef.runtime.local.client.parameters.DefaultNumberOfCores;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.CollectionUtils;
import org.apache.reef.util.MemoryUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import java.io.File;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * Manages a set of Containers that each reference a Thread.
 */
@Private
@DriverSide
final class ContainerManager implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(ContainerManager.class.getName());

  private static final Collection<String> DEFAULT_RACKS = Collections.singletonList(RackNames.DEFAULT_RACK_NAME);

  private final ThreadGroup containerThreads = new ThreadGroup("LocalContainerManagerThreadGroup");


  /**
   * Map from containerID -> Container.
   */
  private final Map<String, Container> containers = new HashMap<>();

  /**
   * Map of free, unallocated nodes by rack, by their Node ID.
   * <RackName,<NodeId, True>>
   * Used a map instead of a list as the value for faster lookup
   */
  private final Map<String, Map<String, Boolean>> freeNodesPerRack = new HashMap<>();

  /**
   * Inverted index, map of <NodeId, RackName>.
   */
  private final Map<String, String> racksPerNode = new HashMap<>();

  /**
   * Capacity of each rack (as even as possible).
   */
  private final Map<String, Integer> capacitiesPerRack = new HashMap<>();

  private final int capacity;
  private final int defaultMemorySize;
  private final int defaultNumberOfCores;
  private final String errorHandlerRID;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorHandler;
  private final File rootFolder;
  private final REEFFileNames fileNames;
  private final ReefRunnableProcessObserver processObserver;
  private final String localAddress;
  private final Collection<String> availableRacks;

  @Inject
  private ContainerManager(
      final RemoteManager remoteManager,
      final REEFFileNames fileNames,
      @Parameter(MaxNumberOfEvaluators.class) final int capacity,
      @Parameter(RootFolder.class) final String rootFolderName,
      @Parameter(RuntimeParameters.NodeDescriptorHandler.class) final
      EventHandler<NodeDescriptorEvent> nodeDescriptorHandler,
      @Parameter(RackNames.class) final Set<String> rackNames,
      final ReefRunnableProcessObserver processObserver,
      final LocalAddressProvider localAddressProvider,
      @Parameter(DefaultMemorySize.class) final int defaultMemorySize,
      @Parameter(DefaultNumberOfCores.class) final int defaultNumberOfCores) {

    this.capacity = capacity;
    this.defaultMemorySize = defaultMemorySize;
    this.defaultNumberOfCores = defaultNumberOfCores;
    this.fileNames = fileNames;
    this.processObserver = processObserver;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.nodeDescriptorHandler = nodeDescriptorHandler;
    this.rootFolder = new File(rootFolderName);
    this.localAddress = localAddressProvider.getLocalAddress();
    this.availableRacks = normalize(rackNames);

    LOG.log(Level.FINEST, "Initializing Container Manager with {0} containers", capacity);

    remoteManager.registerHandler(ReefServiceProtos.RuntimeErrorProto.class,
        new EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>>() {
          @Override
          public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> value) {
            final FailedRuntime error = new FailedRuntime(value.getMessage());
            LOG.log(Level.SEVERE, "FailedRuntime: " + error, error.getReason().orElse(null));
            release(error.getId());
          }
        });

    init();

    LOG.log(Level.FINE, "Initialized Container Manager with {0} containers", capacity);
  }

  /**
   * Normalize rack names. Make sure that each rack name starts with a path separator /
   * and does not have a path separator at the end. Also check that no rack names
   * end with a wildcard *, and raise an exception if such rack name occurs in the input.
   * @param rackNames Collection of rack names to normalize.
   * @return Collection of normalized rack names.
   * @throws IllegalArgumentException if validation of some rack names' fails.
   */
  private static Collection<String> normalize(
      final Collection<String> rackNames) throws IllegalArgumentException {

    return normalize(rackNames, true);
  }

  /**
   * Normalize rack names. Make sure that each rack name starts with a path separator /
   * and does not have a path separator at the end. Also, if end validation is on, check
   * that rack name does not end with a wildcard *.
   * @param rackNames Collection of rack names to normalize.
   * @param validateEnd If true, throw an exception if the name ends with ANY (*)
   * @return Collection of normalized rack names.
   * @throws IllegalArgumentException if validation of some rack names' fails.
   */
  private static Collection<String> normalize(
      final Collection<String> rackNames, final boolean validateEnd) throws IllegalArgumentException {

    final List<String> normalizedRackNames = new ArrayList<>(rackNames.size());

    for (String rackName : rackNames) {

      rackName = rackName.trim();
      Validate.notEmpty(rackName, "Rack names cannot be empty");

      // should start with a separator
      if (!rackName.startsWith(Constants.RACK_PATH_SEPARATOR)) {
        rackName = Constants.RACK_PATH_SEPARATOR + rackName;
      }

      // remove the ending separator
      if (rackName.endsWith(Constants.RACK_PATH_SEPARATOR)) {
        rackName = rackName.substring(0, rackName.length() - 1);
      }

      if (validateEnd) {
        Validate.isTrue(!rackName.endsWith(Constants.ANY_RACK));
      }

      normalizedRackNames.add(rackName);
    }

    return normalizedRackNames;
  }

  private void init() {

    // evenly distribute the containers among the racks
    // if rack names are not specified, the default rack will be used, so the denominator will always be > 0
    final int capacityPerRack = this.capacity / this.availableRacks.size();
    int missing = this.capacity % this.availableRacks.size();

    // initialize the freeNodesPerRackList and the capacityPerRack
    for (final String rackName : this.availableRacks) {
      int currentCapacity = capacityPerRack;
      if (missing > 0) {
        ++currentCapacity;
        --missing;
      }
      this.capacitiesPerRack.put(rackName, currentCapacity);
      this.freeNodesPerRack.put(rackName, new HashMap<String, Boolean>());
    }
  }

  synchronized void start() {
    sendNodeDescriptors();
  }

  private void sendNodeDescriptors() {
    final IDMaker idmaker = new IDMaker("Node-");
    int j = 0;
    for (final String rackName : this.availableRacks) {
      final int rackCapacity = this.capacitiesPerRack.get(rackName);
      for (int i = 0; i < rackCapacity; i++) {
        final String id = idmaker.getNextID();
        this.racksPerNode.put(id, rackName);
        this.freeNodesPerRack.get(rackName).put(id, Boolean.TRUE);

        final int totalMemorySizeInMB = MemoryUtils.getTotalPhysicalMemorySizeInMB();
        final int nodeMemorySizeInMB  = (-1 == totalMemorySizeInMB) ? this.defaultMemorySize : totalMemorySizeInMB;

        this.nodeDescriptorHandler.onNext(NodeDescriptorEventImpl.newBuilder()
                .setIdentifier(id)
                .setRackName(rackName)
                .setHostName(this.localAddress)
                .setPort(j)
                .setMemorySize(nodeMemorySizeInMB)
                .build());
        j++;
      }
    }
  }

  private Collection<String> getRackNamesOrDefault(final List<String> rackNames) {
    return CollectionUtils.isNotEmpty(rackNames) ? normalize(rackNames, false) : DEFAULT_RACKS;
  }

  /**
   * Returns the node name of the container to be allocated if it's available,
   * selected from the list of preferred node names.
   * If the list is empty, then an empty optional is returned.
   * @param nodeNames the list of preferred nodes.
   * @return the node name where to allocate the container.
   */
  private Optional<String> getPreferredNode(final List<String> nodeNames) {

    for (final String nodeName : nodeNames) {
      final String possibleRack = this.racksPerNode.get(nodeName);
      if (possibleRack != null && this.freeNodesPerRack.get(possibleRack).containsKey(nodeName)) {
        return Optional.of(nodeName);
      }
    }

    return Optional.empty();
  }

  /**
   * Returns the rack where to allocate the container, selected from the list of
   * preferred rack names. If the list is empty, and there's space in the default
   * rack, then the default rack is returned. The relax locality semantic is
   * enabled if the list of rack names contains '/*', otherwise relax locality
   * is considered disabled.
   *
   * @param rackNames the list of preferred racks.
   * @return the rack name where to allocate the container.
   */
  private Optional<String> getPreferredRack(final List<String> rackNames) {

    for (final String rackName : getRackNamesOrDefault(rackNames)) {

      // if it does not end with the any modifier, then we should do an exact match
      if (!rackName.endsWith(Constants.ANY_RACK)) {
        if (freeNodesPerRack.containsKey(rackName) && freeNodesPerRack.get(rackName).size() > 0) {
          return Optional.of(rackName);
        }
      } else {

        // if ends with the any modifier, we do a prefix match
        for (final String possibleRackName : this.availableRacks) {

          // remove the any modifier
          final String newRackName = rackName.substring(0, rackName.length() - 1);

          if (possibleRackName.startsWith(newRackName) &&
              this.freeNodesPerRack.get(possibleRackName).size() > 0) {
            return Optional.of(possibleRackName);
          }
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Allocates a container based on a request event. First it tries to match a
   * given node, if it cannot, it tries to get a spot in a rack.
   * @param requestEvent resource request event.
   * @return an optional with the container if allocated.
   */
  Optional<Container> allocateContainer(final ResourceRequestEvent requestEvent) {

    Container container = null;
    final Optional<String> nodeName = getPreferredNode(requestEvent.getNodeNameList());

    if (nodeName.isPresent()) {
      container = allocateBasedOnNode(
          requestEvent.getMemorySize().orElse(this.defaultMemorySize),
          requestEvent.getVirtualCores().orElse(this.defaultNumberOfCores),
          nodeName.get());
    } else {
      final Optional<String> rackName = getPreferredRack(requestEvent.getRackNameList());
      if (rackName.isPresent()) {
        container = allocateBasedOnRack(
            requestEvent.getMemorySize().orElse(this.defaultMemorySize),
            requestEvent.getVirtualCores().orElse(this.defaultNumberOfCores),
            rackName.get());
      }
    }

    return Optional.ofNullable(container);
  }

  private Container allocateBasedOnNode(final int megaBytes, final int numberOfCores, final String nodeId) {
    synchronized (this.containers) {
      // get the rack name
      final String rackName = this.racksPerNode.get(nodeId);
      // remove if from the free map
      this.freeNodesPerRack.get(rackName).remove(nodeId);
      // allocate
      return allocate(megaBytes, numberOfCores, nodeId, rackName);
    }
  }

  private Container allocateBasedOnRack(final int megaBytes, final int numberOfCores, final String rackName) {
    synchronized (this.containers) {

      // get the first free nodeId in the rack
      final Iterator<String> it = this.freeNodesPerRack.get(rackName).keySet().iterator();

      if (!it.hasNext()) {
        throw new IllegalArgumentException("There should be a free node in the specified rack " + rackName);
      }

      final String nodeId = it.next();
      it.remove();

      // allocate
      return allocate(megaBytes, numberOfCores, nodeId, rackName);
    }
  }

  private Container allocate(
      final int megaBytes, final int numberOfCores, final String nodeId, final String rackName) {

    final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());

    final File processFolder = new File(this.rootFolder, processID);
    if (!processFolder.exists() && !processFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", processFolder.getAbsolutePath());
    }

    final ProcessContainer container = new ProcessContainer(
        this.errorHandlerRID, nodeId, processID, processFolder, megaBytes,
        numberOfCores, rackName, this.fileNames, this.processObserver, this.containerThreads);

    this.containers.put(container.getContainerID(), container);
    LOG.log(Level.FINE, "Allocated {0}", container.getContainerID());

    return container;
  }

  void release(final String containerID) {
    synchronized (this.containers) {
      final Container ctr = this.containers.get(containerID);
      if (null != ctr) {
        LOG.log(Level.INFO, "Releasing Container with containerId [{0}]", ctr);
        if (ctr.isRunning()) {
          ctr.close();
        }
        this.freeNodesPerRack.get(ctr.getRackName()).put(ctr.getNodeID(), Boolean.TRUE);
        this.containers.remove(ctr.getContainerID());
      } else {
        LOG.log(Level.INFO, "Ignoring release request for unknown containerID [{0}]", containerID);
      }
    }
  }

  Container get(final String containedID) {
    synchronized (this.containers) {
      return this.containers.get(containedID);
    }
  }

  /**
   * @return a List of the IDs of currently allocated Containers.
   */
  Iterable<String> getAllocatedContainerIDs() {
    return this.containers.keySet();
  }

  @Override
  public synchronized void close() {
    synchronized (this.containers) {
      if (this.containers.isEmpty()) {
        LOG.log(Level.FINEST, "Clean shutdown with no outstanding containers.");
      } else {
        LOG.log(Level.WARNING, "Dirty shutdown with {0} outstanding containers.", this.containers.size());
        for (final Container c : this.containers.values()) {
          LOG.log(Level.WARNING, "Force shutdown of container: {0}", c);
          c.close();
        }
      }
    }
  }
}
