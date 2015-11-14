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
package org.apache.reef.io.network.group.impl.operators;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.exception.ParentDeadException;
import org.apache.reef.io.network.group.api.operators.Gather;
import org.apache.reef.io.network.group.api.task.CommGroupNetworkHandler;
import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.api.task.OperatorTopology;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.task.OperatorTopologyImpl;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatherReceiver<T> implements Gather.Receiver<T>, EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(GatherReceiver.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final Codec<T> dataCodec;
  private final OperatorTopology topology;
  private final CommunicationGroupServiceClient commGroupClient;
  private final AtomicBoolean init = new AtomicBoolean(false);
  private final int version;

  @Inject
  public GatherReceiver(@Parameter(CommunicationGroupName.class) final String groupName,
                        @Parameter(OperatorName.class) final String operName,
                        @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
                        @Parameter(DataCodec.class) final Codec<T> dataCodec,
                        @Parameter(DriverIdentifierGroupComm.class) final String driverId,
                        @Parameter(TaskVersion.class) final int version,
                        final CommGroupNetworkHandler commGroupNetworkHandler,
                        final NetworkService<GroupCommunicationMessage> netService,
                        final CommunicationGroupServiceClient commGroupClient) {
    LOG.finest(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
    this.version = version;
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.topology = new OperatorTopologyImpl(this.groupName, this.operName,
                                             selfId, driverId, new Sender(netService), version);
    this.commGroupClient = commGroupClient;
    commGroupNetworkHandler.register(this.operName, this);
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public void initialize() throws ParentDeadException {
    topology.initialize();
  }

  @Override
  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  @Override
  public Class<? extends Name<String>> getGroupName() {
    return groupName;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatherReceiver:")
        .append(Utils.simpleName(groupName))
        .append(":")
        .append(Utils.simpleName(operName))
        .append(":")
        .append(version);
    return sb.toString();
  }

  @Override
  public void onNext(final GroupCommunicationMessage msg) {
    topology.handle(msg);
  }

  @Override
  public List<T> receive() throws NetworkException, InterruptedException {
    LOG.entering("GatherReceiver", "receive");
    final Map<String, T> mapOfTaskIdToData = receiveMapOfTaskIdToData();

    LOG.log(Level.FINE, "{0} Sorting data according to lexicographical order of task identifiers.", this);
    final TreeMap<String, T> sortedMapOfTaskIdToData = new TreeMap<>(mapOfTaskIdToData);
    final List<T> retList = new LinkedList<>(sortedMapOfTaskIdToData.values());

    LOG.exiting("GatherReceiver", "receive");
    return retList;
  }

  @Override
  public List<T> receive(final List<? extends Identifier> order) throws NetworkException, InterruptedException {
    LOG.entering("GatherReceiver", "receive");
    final Map<String, T> mapOfTaskIdToData = receiveMapOfTaskIdToData();

    LOG.log(Level.FINE, "{0} Sorting data according to specified order of task identifiers.", this);
    final List<T> retList = new LinkedList<>();
    for (final Identifier key : order) {
      final String keyString = key.toString();
      if (mapOfTaskIdToData.containsKey(keyString)) {
        retList.add(mapOfTaskIdToData.get(key.toString()));
      } else {
        LOG.warning(this + " Received no data from " + keyString + ". Adding null.");
        retList.add(null);
      }
    }

    LOG.exiting("GatherReceiver", "receive");
    return retList;
  }

  private Map<String, T> receiveMapOfTaskIdToData() {
    LOG.entering("GatherReceiver", "receiveMapOfTaskIdToData");
    // I am root.
    LOG.fine("I am " + this);

    if (init.compareAndSet(false, true)) {
      LOG.fine(this + " Communication group initializing.");
      commGroupClient.initialize();
      LOG.fine(this + " Communication group initialized.");
    }

    final Map<String, T> mapOfTaskIdToData = new HashMap<>();
    try {
      LOG.fine(this + " Waiting for children.");
      final byte[] gatheredDataFromChildren = topology.recvFromChildren();

      LOG.fine("Using " + dataCodec.getClass().getSimpleName() + " as codec.");
      try (final ByteArrayInputStream bstream = new ByteArrayInputStream(gatheredDataFromChildren);
           final DataInputStream dstream = new DataInputStream(bstream)) {
        while (dstream.available() > 0) {
          final String identifier = dstream.readUTF();
          final int dataLength = dstream.readInt();
          final byte[] data = new byte[dataLength];
          dstream.readFully(data);
          mapOfTaskIdToData.put(identifier, dataCodec.decode(data));
        }
        LOG.fine(this + " Successfully received gathered data.");
      }

    } catch (final ParentDeadException e) {
      throw new RuntimeException("ParentDeadException", e);
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }

    LOG.exiting("GatherReceiver", "receiveMapOfTaskIdToData");
    return mapOfTaskIdToData;
  }
}
