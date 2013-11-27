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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.RuntimeError;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.RemoteMessage;
import com.microsoft.wake.time.Time;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

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

  private final static Logger LOG = Logger.getLogger(ContainerManager.class.getName());

  /**
   * Map from containerID -> Container
   */
  private final Map<String, Container> containers = new HashMap<>();

  /**
   * List of free, unallocated nodes by their Node ID
   */
  private final List<String> freeNodeList = new LinkedList<>();

  private final String errorHandlerRID;
  private final int capacity;
  private final EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> nodeDescriptorHandler;
  private final File rootFolder;

  @Inject
  ContainerManager(final RemoteManager remoteManager, final RuntimeClock clock,
                   @Parameter(LocalRuntimeConfiguration.NumberOfThreads.class) final int capacity,
                   @Parameter(LocalRuntimeConfiguration.RootFolder.class) final String rootFolderName,
                   @Parameter(RuntimeParameters.NodeDescriptorHandler.class) final EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> nodeDescriptorHandler) {
    this.capacity = capacity;
    this.errorHandlerRID = remoteManager.getMyIdentifier();
    this.nodeDescriptorHandler = nodeDescriptorHandler;
    this.rootFolder = new File(rootFolderName);

    LOG.log(Level.FINEST, "Initializing Container Manager with {0} containers", capacity);

    remoteManager.registerHandler(ReefServiceProtos.RuntimeErrorProto.class, new EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>>() {
      @Override
      public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> value) {
        final RuntimeError error = new RuntimeError(value.getMessage());
        LOG.log(Level.SEVERE, "RuntimeError: " + error, error.getCause());
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

    LOG.log(Level.INFO, "Initialized Container Manager with {0} containers", capacity);
  }

  private void sendNodeDescriptors() {
    final IDMaker idmaker = new IDMaker("Node-");
    for (int i = 0; i < capacity; i++) {
      final String id = idmaker.getNextID();
      this.freeNodeList.add(id);
      nodeDescriptorHandler.onNext(DriverRuntimeProtocol.NodeDescriptorProto.newBuilder()
          .setIdentifier(id)
          .setRackName("/default-rack")
          .setHostName(NetUtils.getLocalAddress())
          .setPort(i)
          .setMemorySize(512)
          .build());
    }
  }

  final boolean hasContainerAvailable() {
    return this.freeNodeList.size() > 0;
  }

  final Container allocateOne() {
    synchronized (this.containers) {
      final String nodeId = this.freeNodeList.remove(0);
      final String processID = nodeId + "-" + String.valueOf(System.currentTimeMillis());
      final File processFolder = new File(this.rootFolder, processID);
      processFolder.mkdirs();
      final ProcessContainer container = new ProcessContainer(this.errorHandlerRID, nodeId, processID, processFolder);
      this.containers.put(container.getContainerID(), container);
      return container;
    }
  }

  final void release(final String containerID) {
    synchronized (this.containers) {
      final Container ctr = this.containers.get(containerID);
      LOG.info("Releasing: " + ctr);
      ctr.close();
      this.freeNodeList.add(ctr.getNodeID());
      this.containers.remove(ctr.getContainerID());
    }
  }


  final Container get(final String containedID) {
    synchronized (this.containers) {
      return this.containers.get(containedID);
    }
  }

  /**
   * @return a List of the IDs of currently allocated Containers.
   */
  final Iterable<String> getAllocatedContainerIDs() {
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
          LOG.log(Level.WARNING, "Force shutdown of:" + c);
          c.close();
        }
      }
    }
  }
}
