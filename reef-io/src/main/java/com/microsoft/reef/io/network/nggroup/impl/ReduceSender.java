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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopology;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.DataCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.TaskVersion;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class ReduceSender<T> implements Reduce.Sender<T>, EventHandler<GroupCommMessage> {

  private static final Logger LOG = Logger.getLogger(ReduceSender.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final CommGroupNetworkHandler commGroupNetworkHandler;
  private final Codec<T> dataCodec;
  private final NetworkService<GroupCommMessage> netService;
  private final Sender sender;
  private final ReduceFunction<T> reduceFunction;

  private final OperatorTopology topology;

  private final CommunicationGroupClient commGroupClient;

  private final AtomicBoolean init = new AtomicBoolean(false);

  private final int version;

  @Inject
  public ReduceSender(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(OperatorName.class) final String operName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String selfId,
      @Parameter(DataCodec.class) final Codec<T> dataCodec,
      @Parameter(com.microsoft.reef.io.network.nggroup.impl.config.parameters.ReduceFunctionParam.class) final ReduceFunction<T> reduceFunction,
      @Parameter(DriverIdentifier.class) final String driverId,
      @Parameter(TaskVersion.class) final int version,
      final CommGroupNetworkHandler commGroupNetworkHandler,
      final NetworkService<GroupCommMessage> netService,
      final CommunicationGroupClient commGroupClient) {
    super();
    this.version = version;
    LOG.info(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
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
  public void initialize() {
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
  public void onNext(final GroupCommMessage msg) {
    topology.handle(msg);
  }

  @Override
  public void send(final T myData) throws NetworkException, InterruptedException {
    if (init.compareAndSet(false, true)) {
      commGroupClient.initialize();
    }
    //I am an intermediate node or leaf.
    LOG.log(Level.INFO, "I am Reduce sender" + topology.getSelfId() + " for oper: " + operName + " in group " + groupName);
    LOG.info("Waiting for children");
    //Wait for children to send
    final List<byte[]> valBytes = topology.recvFromChildren();


    final List<T> vals = new ArrayList<T>(valBytes.size() + 1);
    vals.add(myData);
    for (final byte[] data : valBytes) {
      vals.add(dataCodec.decode(data));
    }

    //Reduce the received values
    final T reducedValue = reduceFunction.apply(vals);
    LOG.log(Level.INFO, "Sending " + reducedValue + " to parent");
    topology.sendToParent(dataCodec.encode(reducedValue), Type.Reduce);
  }

  @Override
  public ReduceFunction<T> getReduceFunction() {
    return reduceFunction;
  }

}
