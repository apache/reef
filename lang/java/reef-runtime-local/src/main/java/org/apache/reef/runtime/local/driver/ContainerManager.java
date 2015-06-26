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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.runtime.local.client.parameters.RackNames;
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  /**
   * Map from containerID -> Container.
   */
  private final Map<String, Container> containers = new HashMap<>();

  /**
   * Map of free, unallocated nodes by rack, by their Node ID.
   */
  private final Map<String, List<String>> freeNodesPerRack = new HashMap<>();
  /**
   * Capacity of each rack (as even as possible)
   */
  private final Map<String, Integer> capacityPerRack = new HashMap<>();

  private final String errorHandlerRID;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorHandler;
  private final File rootFolder;
  private final REEFFileNames fileNames;
  private final ReefRunnableProcessObserver processObserver;
  private final String localAddress;
  private final Set<String> availableRacks;

  @Inject
  ContainerManager(
      final RemoteManager remoteManager,
      final RuntimeClock clock,
      final REEFFileNames fileNames,
      @Parameter(MaxNumberOfEvaluators.class) final int capacity,
      @Parameter(RootFolder.class) final String rootFolderName,
      @Parameter(RuntimeParameters.NodeDescriptorHandler.class) final
      EventHandler<NodeDescriptorEvent> nodeDescriptorHandler,
      @Parameter(RackNames.class) final Set<String> rackNames,
      final ReefRunnableProcessObserver processObserver,
      final LocalAddressProvider localAddressProvider) {
    this.fileNames = fileNames;
    this.processObserver = processObserver;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.nodeDescriptorHandler = nodeDescriptorHandler;
    this.rootFolder = new File(rootFolderName);
    this.localAddress = localAddressProvider.getLocalAddress();
    this.availableRacks = rackNames;

    LOG.log(Level.FINEST, "Initializing Container Manager with {0} containers", capacity);

    remoteManager.registerHandler(ReefServiceProtos.RuntimeErrorProto.class, new EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>>() {
      @Override
      public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> value) {
        final FailedRuntime error = new FailedRuntime(value.getMessage());
        LOG.log(Level.SEVERE, "FailedRuntime: " + error, error.getReason().orElse(null));
        release(error.getId());
      }
    });
    clock.registerEventHandler(RuntimeStart.class, new EventHandler<Time>() {
      @Override
      public void onNext(final Time value) {
        synchronized (ContainerManager.this) {
          ContainerManager.this.sendNodeDescriptors();
        }
      }
    });

    clock.registerEventHandler(RuntimeStop.class, new EventHandler<Time>() {
      @Override
      public void onNext(final Time value) {
        synchronized (ContainerManager.this) {
          LOG.log(Level.FINEST, "RuntimeStop: close the container manager");
          ContainerManager.this.close();
        }
      }
    });

    init(capacity, rackNames);

    LOG.log(Level.FINE, "Initialized Container Manager with {0} containers", capacity);
  }


  private void init(final int capacity, final Set<String> rackNames) {
    // evenly distribute the containers among the racks
    // if rack names are not specified, the default rack will be used, so the denominator will always be > 0
    final int capacityPerRack = capacity / rackNames.size();
    int missing = capacity % rackNames.size();
    // initialize the freeNodesPerRackList and the capacityPerRack
    for (final String rackName : rackNames) {
      this.freeNodesPerRack.put(rackName, new ArrayList<String>());
      this.capacityPerRack.put(rackName, capacityPerRack);
      if (missing > 0) {
        this.capacityPerRack.put(rackName, this.capacityPerRack.get(rackName) + 1);
        missing--;
      }
    }
  }


  private void sendNodeDescriptors() {
    final IDMaker idmaker = new IDMaker("Node-");
    int j = 0;
    for (final String rackName : this.availableRacks) {
      final int rackCapacity = this.capacityPerRack.get(rackName);
      for (int i = 0; i < rackCapacity; i++) {
        final String id = idmaker.getNextID();
        this.freeNodesPerRack.get(rackName).add(id);
        this.nodeDescriptorHandler.onNext(NodeDescriptorEventImpl.newBuilder()
            .setIdentifier(id)
            .setRackName(rackName)
            .setHostName(this.localAddress)
            .setPort(j)
            .setMemorySize(512) // TODO: Find the actual system memory on this machine.
            .build());
        j++;
      }
    }
  }

  boolean hasContainerAvailable(final String rackName) {
    // if rack name does not exist, return false
    if (!freeNodesPerRack.containsKey(rackName)) {
      return false;
    }
    return this.freeNodesPerRack.get(rackName).size() > 0;
  }

  Container allocateOne(final int megaBytes, final int numberOfCores, final String rackName) {
    synchronized (this.containers) {
      final String nodeId = this.freeNodesPerRack.get(rackName).remove(0);
      final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());
      final File processFolder = new File(this.rootFolder, processID);
      processFolder.mkdirs();
      final ProcessContainer container = new ProcessContainer(
          this.errorHandlerRID, nodeId, processID, processFolder, megaBytes, numberOfCores, rackName, this.fileNames, this.processObserver);
      this.containers.put(container.getContainerID(), container);
      LOG.log(Level.FINE, "Allocated {0}", container.getContainerID());
      return container;
    }
  }

  void release(final String containerID) {
    synchronized (this.containers) {
      final Container ctr = this.containers.get(containerID);
      if (null != ctr) {
        LOG.log(Level.INFO, "Releasing Container with containerId [{0}]", ctr);
        if (ctr.isRunning()) {
          ctr.close();
        }
        this.freeNodesPerRack.get(ctr.getRackName()).add(ctr.getNodeID());
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
  public void close() {
    synchronized (this.containers) {
      if (this.containers.isEmpty()) {
        LOG.log(Level.FINEST, "Clean shutdown with no outstanding containers.");
      } else {
        LOG.log(Level.WARNING, "Dirty shutdown with outstanding containers.");
        for (final Container c : this.containers.values()) {
          LOG.log(Level.WARNING, "Force shutdown of: {0}", c);
          c.close();
        }
      }
    }
  }
}