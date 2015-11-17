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
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class GatherSender<T> implements Gather.Sender<T>, EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(GatherSender.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final Codec<T> dataCodec;
  private final NetworkService<GroupCommunicationMessage> netService;
  private final OperatorTopology topology;
  private final CommunicationGroupServiceClient commGroupClient;
  private final AtomicBoolean init = new AtomicBoolean(false);
  private final int version;

  @Inject
  public GatherSender(@Parameter(CommunicationGroupName.class) final String groupName,
                      @Parameter(OperatorName.class) final String operName,
                      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
                      @Parameter(DataCodec.class) final Codec<T> dataCodec,
                      @Parameter(DriverIdentifierGroupComm.class) final String driverId,
                      @Parameter(TaskVersion.class) final int version,
                      final CommGroupNetworkHandler commGroupNetworkHandler,
                      final NetworkService<GroupCommunicationMessage> netService,
                      final CommunicationGroupServiceClient commGroupClient) {
    LOG.finest(operName + "has CommGroupHandler-" + commGroupNetworkHandler.toString());
    this.version = version;
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.netService = netService;
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
    final StringBuilder sb = new StringBuilder("GatherSender:")
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
  public void send(final T myData) throws NetworkException, InterruptedException {
    LOG.entering("GatherSender", "send", myData);
    // I am an intermediate node or a leaf.
    LOG.fine("I am " + this);

    if (init.compareAndSet(false, true)) {
      LOG.fine(this + " Communication group initializing.");
      commGroupClient.initialize();
      LOG.fine(this + " Communication group initialized.");
    }

    try {
      LOG.finest(this + " Waiting for children.");
      final byte[] gatheredData = topology.recvFromChildren();
      final byte[] encodedMyData = dataCodec.encode(myData);

      try (final ByteArrayOutputStream bstream = new ByteArrayOutputStream();
           final DataOutputStream dstream = new DataOutputStream(bstream)) {
        dstream.writeUTF(netService.getMyId().toString());
        dstream.writeInt(encodedMyData.length);
        dstream.write(encodedMyData);
        dstream.write(gatheredData);
        final byte[] mergedData = bstream.toByteArray();

        LOG.fine(this + " Sending merged value to parent.");
        topology.sendToParent(mergedData, ReefNetworkGroupCommProtos.GroupCommMessage.Type.Gather);
      }
    } catch (final ParentDeadException e) {
      throw new RuntimeException("ParentDeadException", e);
    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
    LOG.exiting("GatherSender", "send");
  }
}
