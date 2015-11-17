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
import org.apache.reef.io.network.group.api.operators.Scatter;
import org.apache.reef.io.network.group.api.task.CommGroupNetworkHandler;
import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.api.task.OperatorTopology;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.task.OperatorTopologyImpl;
import org.apache.reef.io.network.group.impl.utils.ScatterEncoder;
import org.apache.reef.io.network.group.impl.utils.ScatterHelper;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public final class ScatterSender<T> implements Scatter.Sender<T>, EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(ScatterSender.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final Codec<T> dataCodec;
  private final OperatorTopology topology;
  private final AtomicBoolean init = new AtomicBoolean(false);
  private final CommunicationGroupServiceClient commGroupClient;
  private final int version;
  private final ScatterEncoder scatterEncoder;

  @Inject
  public ScatterSender(@Parameter(CommunicationGroupName.class) final String groupName,
                       @Parameter(OperatorName.class) final String operName,
                       @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
                       @Parameter(DataCodec.class) final Codec<T> dataCodec,
                       @Parameter(DriverIdentifierGroupComm.class) final String driverId,
                       @Parameter(TaskVersion.class) final int version,
                       final CommGroupNetworkHandler commGroupNetworkHandler,
                       final NetworkService<GroupCommunicationMessage> netService,
                       final CommunicationGroupServiceClient commGroupClient,
                       final ScatterEncoder scatterEncoder) {
    LOG.finest(operName + "has CommGroupHandler-" + commGroupNetworkHandler.toString());
    this.version = version;
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.scatterEncoder = scatterEncoder;
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
    final StringBuilder sb = new StringBuilder("ScatterSender:")
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

  private void initializeGroup() {
    if (init.compareAndSet(false, true)) {
      LOG.fine(this + " Communication group initializing.");
      commGroupClient.initialize();
      LOG.fine(this + " Communication group initialized.");
    }
  }

  @Override
  public void send(final List<T> elements) throws NetworkException, InterruptedException {
    LOG.entering("ScatterSender", "send");

    initializeGroup();
    send(elements,
        ScatterHelper.getUniformCounts(elements.size(), commGroupClient.getActiveSlaveTasks().size()),
        commGroupClient.getActiveSlaveTasks());

    LOG.exiting("ScatterSender", "send");
  }

  @Override
  public void send(final List<T> elements, final Integer... counts)
      throws NetworkException, InterruptedException {
    LOG.entering("ScatterSender", "send");

    initializeGroup();
    if (counts.length != commGroupClient.getActiveSlaveTasks().size()) {
      throw new RuntimeException("Parameter 'counts' has length " + counts.length
          + ", but number of slaves is " + commGroupClient.getActiveSlaveTasks().size());
    }

    send(elements,
        Arrays.asList(counts),
        commGroupClient.getActiveSlaveTasks());

    LOG.exiting("ScatterSender", "send");
  }

  @Override
  public void send(final List<T> elements, final List<? extends Identifier> order)
      throws NetworkException, InterruptedException {
    LOG.entering("ScatterSender", "send");

    initializeGroup();
    send(elements,
        ScatterHelper.getUniformCounts(elements.size(), order.size()),
        order);

    LOG.exiting("ScatterSender", "send");
  }

  @Override
  public void send(final List<T> elements, final List<Integer> counts, final List<? extends Identifier> order)
      throws NetworkException, InterruptedException {
    LOG.entering("ScatterSender", "send");

    if (counts.size() != order.size()) {
      throw new RuntimeException("Parameter 'counts' has size " + counts.size()
          + ", but parameter 'order' has size " + order.size() + ".");
    }
    initializeGroup();

    // I am root.
    LOG.fine("I am " + this);

    LOG.fine(this + " Encoding data and determining which Tasks receive which elements.");
    final Map<String, byte[]> mapOfChildIdToBytes = scatterEncoder.encode(elements, counts, order, dataCodec);

    try {
      LOG.fine(this + " Sending " + elements.size() + " elements.");
      topology.sendToChildren(mapOfChildIdToBytes, ReefNetworkGroupCommProtos.GroupCommMessage.Type.Scatter);

    } catch (final ParentDeadException e) {
      throw new RuntimeException("ParentDeadException during OperatorTopology.sendToChildren()", e);
    }

    LOG.exiting("ScatterSender", "send");
  }
}
