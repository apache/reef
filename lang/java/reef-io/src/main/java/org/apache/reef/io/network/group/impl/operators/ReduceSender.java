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
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.group.api.task.CommGroupNetworkHandler;
import org.apache.reef.io.network.group.api.task.CommunicationGroupServiceClient;
import org.apache.reef.io.network.group.api.task.OperatorTopology;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.task.OperatorTopologyImpl;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReduceSender<T> implements Reduce.Sender<T>, EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(ReduceSender.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final NetworkService<GroupCommunicationMessage> netService;
  private final Sender sender;
  private final ReduceFunction<T> reduceFunction;

  private final OperatorTopology topology;

  private final CommunicationGroupServiceClient commGroupClient;

  private final AtomicBoolean init = new AtomicBoolean(false);

  private final int version;

  @Inject
  public ReduceSender(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
      @Parameter(ReduceFunctionParam.class) final ReduceFunction<T> reduceFunction,
      @Parameter(DriverIdentifierGroupComm.class) final String driverId,
      @Parameter(TaskVersion.class) final int version,
      final CommGroupNetworkHandler commGroupNetworkHandler,
      final NetworkService<GroupCommunicationMessage> netService,
      final CommunicationGroupServiceClient commGroupClient) {

    super();

    LOG.log(Level.FINEST, "{0} has CommGroupHandler-{1}",
        new Object[]{operName, commGroupNetworkHandler});

    this.version = version;
    this.groupName = Utils.getClass(groupName);
    this.operName = Utils.getClass(operName);
    this.dataCodec = dataCodec;
    this.reduceFunction = reduceFunction;
    this.commGroupNetworkHandler = commGroupNetworkHandler;
    this.netService = netService;
    this.sender = new Sender(this.netService);
    this.topology = new OperatorTopologyImpl(this.groupName, this.operName, selfId, driverId, sender, version);
    this.commGroupNetworkHandler.register(this.operName, this);
    this.commGroupClient = commGroupClient;
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
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":" + version;
  }

  @Override
  public void onNext(final GroupCommunicationMessage msg) {
    topology.handle(msg);
  }

  @Override
  public void send(final T myData) throws NetworkException, InterruptedException {
    LOG.entering("ReduceSender", "send", this);
    LOG.fine("I am " + this);

    if (init.compareAndSet(false, true)) {
      commGroupClient.initialize();
    }
    // I am an intermediate node or leaf.
    LOG.finest("Waiting for children");
    // Wait for children to send
    try {
      final T reducedValueOfChildren = topology.recvFromChildren(reduceFunction, dataCodec);
      final List<T> vals = new ArrayList<>(2);
      vals.add(myData);
      if (reducedValueOfChildren != null) {
        vals.add(reducedValueOfChildren);
      }
      final T reducedValue = reduceFunction.apply(vals);
      topology.sendToParent(dataCodec.encode(reducedValue), ReefNetworkGroupCommProtos.GroupCommMessage.Type.Reduce);
    } catch (final ParentDeadException e) {
      throw new RuntimeException("ParentDeadException", e);
    }
    LOG.exiting("ReduceSender", "send", this);
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
  }
}
