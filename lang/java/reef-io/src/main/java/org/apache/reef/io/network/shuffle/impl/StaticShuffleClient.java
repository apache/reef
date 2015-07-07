/**
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
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.shuffle.topology.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.utils.BaseTupleOperatorFactory;
import org.apache.reef.io.network.shuffle.utils.ClientConfigurationDeserializer;
import org.apache.reef.io.network.shuffle.utils.TupleMessageDispatcher;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.*;

/**
 *
 */
public final class StaticShuffleClient implements ShuffleClient {

  private boolean isTopologySetup;

  private final Class<? extends Name<String>> shuffleName;

  private final TupleMessageDispatcher tupleMessageDispatcher;

  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;

  private final List<String> groupingNameList;
  private final Map<String, GroupingDescriptor> groupingDescriptorMap;
  private final Map<String, List<String>> senderIdListMap;
  private final Map<String, List<String>> receiverIdListMap;
  private final Map<String, StreamingCodec<Tuple>> tupleCodecMap;

  private final BaseTupleOperatorFactory operatorFactory;
  private final Map<String, BaseTupleReceiver> receiverMap;
  private final Map<String, BaseTupleSender> senderMap;

  @Inject
  public StaticShuffleClient(
      final @Parameter(SerializedShuffleName.class) String serializedShuffleName,
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final NetworkServiceClient nsClient,
      final Injector injector,
      final ClientConfigurationDeserializer deserializer) {

    try {
      this.shuffleName = (Class<? extends Name<String>>) Class.forName(serializedShuffleName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.tupleMessageDispatcher = new TupleMessageDispatcher();

    this.isTopologySetup = false;

    this.groupingDescriptorMap = deserializer.getGroupingDescriptorMap();
    this.senderIdListMap = deserializer.getSenderIdListMap();
    this.receiverIdListMap = deserializer.getReceiverIdListMap();
    this.groupingNameList = deserializer.getGroupingNameList();
    this.tupleCodecMap = deserializer.getTupleCodecMap();

    this.senderMap = new HashMap<>();
    this.receiverMap = new HashMap<>();

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();

    final ConnectionFactory<ShuffleTupleMessage> connFactory
        = nsClient.getConnectionFactory(ShuffleTupleMessageNSId.class);

    this.operatorFactory = new BaseTupleOperatorFactory(taskId, this, connFactory, injector);
  }


  @Override
  public Class<? extends Name<String>> getShuffleName() {
    return shuffleName;
  }

  @Override
  public List<String> getGroupingNameList() {
    return groupingNameList;
  }

  @Override
  public GroupingDescriptor getGroupingDescriptor(String groupingName) {
    return groupingDescriptorMap.get(groupingName);
  }

  @Override
  public List<String> getSenderIdList(String groupingName) {
    return senderIdListMap.get(groupingName);
  }

  @Override
  public List<String> getReceiverIdList(String groupingName) {
    return receiverIdListMap.get(groupingName);
  }

  @Override
  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return controlMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return controlLinkListener;
  }

  @Override
  public boolean waitForTopologySetup() {
    synchronized(this) {
      if (!isTopologySetup) {
        try {
          while (!isTopologySetup) {
            this.wait();
          }
        } catch (final InterruptedException e) {
          throw new RuntimeException("An InterruptedException occurred while waiting for topology set up", e);
        }

        return true;
      }
    }
    return false;
  }

  @Override
  public EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler() {
    return tupleMessageDispatcher.getTupleMessageHandler();
  }

  @Override
  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return tupleMessageDispatcher.getTupleLinkListener();
  }

  @Override
  public StreamingCodec<Tuple> getTupleCodec(String groupingName) {
    return tupleCodecMap.get(groupingName);
  }

  @Override
  public <K, V> ShuffleTupleReceiver<K, V> getReceiver(final String groupingName) {
    if (!receiverMap.containsKey(groupingName)) {
      receiverMap.put(groupingName, operatorFactory.createReceiverWith(groupingDescriptorMap.get(groupingName)));
    }

    return receiverMap.get(groupingName);
  }

  @Override
  public <K, V> ShuffleTupleSender<K, V> getSender(final String groupingName) {
    if (!senderMap.containsKey(groupingName)) {
      senderMap.put(groupingName, operatorFactory.createSenderWith(groupingDescriptorMap.get(groupingName)));
    }

    return senderMap.get(groupingName);
  }

  @Override
  public <K, V> void registerLinkListener(final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    tupleMessageDispatcher.registerLinkListener(groupingName, linkListener);
  }

  @Override
  public <K, V> void registerMessageHandler(final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    tupleMessageDispatcher.registerMessageHandler(groupingName, messageHandler);
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == StaticShuffleMessageCode.TOPOLOGY_SETUP) {
        synchronized (StaticShuffleClient.this) {
          isTopologySetup = true;
          StaticShuffleClient.this.notifyAll();
        }
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {
    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
    }
  }
}
