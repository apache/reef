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
import org.apache.reef.runtime.local.client.parameters.RootFolder;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
   * List of free, unallocated nodes by their Node ID.
   */
  private final List<String> freeNodeList = new LinkedList<>();

  private final String errorHandlerRID;
  private final int capacity;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorHandler;
  private final File rootFolder;
  private final REEFFileNames fileNames;
  private final ReefRunnableProcessObserver processObserver;
  private final String localAddress;

  @Inject
  ContainerManager(
      final RemoteManager remoteManager,
      final RuntimeClock clock,
      final REEFFileNames fileNames,
      @Parameter(MaxNumberOfEvaluators.class) final int capacity,
      @Parameter(RootFolder.class) final String rootFolderName,
      @Parameter(RuntimeParameters.NodeDescriptorHandler.class) final 
      EventHandler<NodeDescriptorEvent> nodeDescriptorHandler,
      final ReefRunnableProcessObserver processObserver,
      final LocalAddressProvider localAddressProvider) {
    this.capacity = capacity;
    this.fileNames = fileNames;
    this.processObserver = processObserver;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.nodeDescriptorHandler = nodeDescriptorHandler;
    this.rootFolder = new File(rootFolderName);
    this.localAddress = localAddressProvider.getLocalAddress();

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

    LOG.log(Level.FINE, "Initialized Container Manager with {0} containers", capacity);
  }

  private void sendNodeDescriptors() {
    final IDMaker idmaker = new IDMaker("Node-");
    for (int i = 0; i < capacity; i++) {
      final String id = idmaker.getNextID();
      this.freeNodeList.add(id);
      nodeDescriptorHandler.onNext(NodeDescriptorEventImpl.newBuilder()
          .setIdentifier(id)
          .setRackName("/default-rack")
          .setHostName(this.localAddress)
          .setPort(i)
          .setMemorySize(512) // TODO: Find the actual system memory on this machine.
          .build());
    }
  }

  boolean hasContainerAvailable() {
    return this.freeNodeList.size() > 0;
  }

  Container allocateOne(final int megaBytes, final int numberOfCores) {
    synchronized (this.containers) {
      final String nodeId = this.freeNodeList.remove(0);
      final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());
      final File processFolder = new File(this.rootFolder, processID);
      processFolder.mkdirs();
      final ProcessContainer container = new ProcessContainer(
          this.errorHandlerRID, nodeId, processID, processFolder, megaBytes, numberOfCores, this.fileNames, this.processObserver);
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
        this.freeNodeList.add(ctr.getNodeID());
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
