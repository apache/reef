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

import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.grouping.Grouping;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.io.network.shuffle.utils.BroadcastEventHandler;
import org.apache.reef.io.network.shuffle.utils.BroadcastLinkListener;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.*;

/**
 *
 */
public final class ImmutableShuffleTopologyClient implements ShuffleTopologyClient {

  private boolean isTopologySetup;

  private final Class<? extends Name<String>> topologyName;
  private final EventHandler<Message<ShuffleTupleMessage>> tupleMessageHandler;
  private final LinkListener<Message<ShuffleTupleMessage>> tupleLinkListener;
  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;
  private final Map<String, GroupingDescription> groupingDescriptionMap;
  private final Map<String, NodePoolDescription> nodePoolDescriptionMap;
  private final Map<String, NodePoolDescription> senderPoolDescriptionMap;
  private final Map<String, NodePoolDescription> receiverPoolDescriptionMap;

  private final Map<String, StreamingCodec<Tuple>> tupleCodecMap;
  private final Map<String, ShuffleTupleReceiver> receiverMap;
  private final Map<String, ShuffleTupleSender> senderMap;

  private final Map<String, BroadcastEventHandler<Message<ShuffleTupleMessage>>> messageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleTupleMessage>>> linkListenerMap;

  private final ConnectionFactory<ShuffleTupleMessage> connFactory;
  private final Injector injector;
  private final ConfigurationSerializer confSerializer;

  @Inject
  public ImmutableShuffleTopologyClient(
      final @Parameter(ShuffleTopologyName.class) String serializedTopologyName,
      final @Parameter(SerializedNodePoolSet.class) Set<String> nodePoolSet,
      final @Parameter(SerializedGroupingSet.class) Set<String> groupingSet,
      final @Parameter(SerializedSenderNodePoolSet.class) Set<String> senderNodePoolSet,
      final @Parameter(SerializedReceiverNodePoolSet.class) Set<String> receiverNodePoolSet,
      final NetworkServiceClient nsClient,
      final Injector injector,
      final ConfigurationSerializer confSerializer) {

    try {
      this.topologyName = (Class<? extends Name<String>>) Class.forName(serializedTopologyName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.isTopologySetup = false;
    this.connFactory = nsClient.getConnectionFactory(ShuffleTupleMessageNSId.class);
    this.injector = injector;
    this.confSerializer = confSerializer;
    this.groupingDescriptionMap = new HashMap<>();
    this.nodePoolDescriptionMap = new HashMap<>();
    this.tupleCodecMap = new HashMap<>();
    this.senderPoolDescriptionMap = new HashMap<>();
    this.receiverPoolDescriptionMap = new HashMap<>();
    this.senderMap = new HashMap<>();
    this.receiverMap = new HashMap<>();

    this.tupleMessageHandler = new TupleMessageHandler();
    this.tupleLinkListener = new TupleLinkListener();
    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();

    this.messageHandlerMap = new HashMap<>();
    this.linkListenerMap = new HashMap<>();

    deserializeNodePoolSet(nodePoolDescriptionMap, nodePoolSet);
    deserializeNodePoolSet(senderPoolDescriptionMap, senderNodePoolSet);
    deserializeNodePoolSet(receiverPoolDescriptionMap, receiverNodePoolSet);
    deserializeGroupingSet(groupingDescriptionMap, groupingSet);
    createOperators();
  }

  private void deserializeNodePoolSet(final Map<String, NodePoolDescription> descriptionMap, final Set<String> nodePoolSet) {
    for (final String serializedNodePool : nodePoolSet) {
      final NodePoolDescription nodePoolDescription;
      try {
        nodePoolDescription = injector.forkInjector(confSerializer.fromString(serializedNodePool))
            .getInstance(NodePoolDescription.class);
        descriptionMap.put(nodePoolDescription.getNodePoolName(), nodePoolDescription);
      } catch (Exception e) {
        throw new RuntimeException("An error occurred when deserializing a node pool " + serializedNodePool , e);
      }
    }
  }

  private void deserializeGroupingSet(final Map<String, GroupingDescription> descriptionMap, final Set<String> groupingSet) {
    for (final String serializedGrouping : groupingSet) {
      final GroupingDescription groupingDescription;
      try {
        groupingDescription = injector.forkInjector(confSerializer.fromString(serializedGrouping))
            .getInstance(GroupingDescription.class);
        descriptionMap.put(groupingDescription.getGroupingName(), groupingDescription);
      } catch (Exception e) {
        throw new RuntimeException("An error occurred when deserializing a node pool " + serializedGrouping , e);
      }
    }
  }

  private void createOperators() {
    for (final GroupingDescription groupingDescription : groupingDescriptionMap.values()) {
      createTupleCodec(groupingDescription);
      createOperatorsWith(groupingDescription);
    }
  }

  private void createTupleCodec(GroupingDescription groupingDescription) {
    final Configuration codecConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ShuffleKeyCodec.class, groupingDescription.getKeyCodec())
        .bindNamedParameter(ShuffleValueCodec.class, groupingDescription.getValueCodec())
        .build();

    try {
      tupleCodecMap.put(
          groupingDescription.getGroupingName(),
          Tang.Factory.getTang().newInjector(codecConfiguration)
              .getInstance(TupleCodec.class)
      );
    } catch (InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while injecting tuple codec with " + groupingDescription, e);
    }
  }

  private void createOperatorsWith(final GroupingDescription groupingDescription) {
    if (nodePoolDescriptionMap.keySet().contains(groupingDescription.getReceiverPoolId())) {
      createReceiverWith(groupingDescription);
    }

    if (nodePoolDescriptionMap.keySet().contains(groupingDescription.getSenderPoolId())) {
      createSenderWith(groupingDescription);
    }
  }

  private void createReceiverWith(final GroupingDescription groupingDescription) {
    final Injector forkedInjector = injector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleTopologyClient.class, this);
    forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);

    try {
      receiverMap.put(groupingDescription.getGroupingName(), forkedInjector.getInstance(ShuffleTupleReceiver.class));
      messageHandlerMap.put(groupingDescription.getGroupingName(), new BroadcastEventHandler());
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
    }
  }

  private void createSenderWith(final GroupingDescription groupingDescription) {
    final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Grouping.class, groupingDescription.getGroupingClass())
        .build();

    final Injector forkedInjector = injector.forkInjector(senderConfiguration);
    forkedInjector.bindVolatileInstance(ShuffleTopologyClient.class, this);
    forkedInjector.bindVolatileInstance(GroupingDescription.class, groupingDescription);
    forkedInjector.bindVolatileInstance(NodePoolDescription.class, receiverPoolDescriptionMap.get(groupingDescription.getReceiverPoolId()));
    forkedInjector.bindVolatileInstance(ConnectionFactory.class, connFactory);
    forkedInjector.bindVolatileParameter(ShuffleTupleCodec.class, tupleCodecMap.get(groupingDescription.getGroupingName()));

    try {
      senderMap.put(groupingDescription.getGroupingName(), forkedInjector.getInstance(ShuffleTupleSender.class));
      linkListenerMap.put(groupingDescription.getGroupingName(), new BroadcastLinkListener());
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while deserializing sender with " + groupingDescription, e);
    }
  }

  @Override
  public Class<? extends Name<String>> getTopologyName() {
    return topologyName;
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
    return tupleMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return tupleLinkListener;
  }

  @Override
  public Codec<Tuple> getTupleCodec(String groupingName) {
    return tupleCodecMap.get(groupingName);
  }

  @Override
  public Map<String, GroupingDescription> getGroupingDescriptionMap() {
    return groupingDescriptionMap;
  }

  @Override
  public Map<String, NodePoolDescription> getNodePoolDescriptionMap() {
    return nodePoolDescriptionMap;
  }

  @Override
  public Map<String, NodePoolDescription> getSenderPoolDescriptionMap() {
    return senderPoolDescriptionMap;
  }

  @Override
  public Map<String, NodePoolDescription> getReceiverPoolDescriptionMap() {
    return receiverPoolDescriptionMap;
  }


  @Override
  public <K, V> ShuffleTupleReceiver<K, V> getReceiver(final String groupingName) {
    if (!receiverMap.containsKey(groupingName)) {
      throw new RuntimeException("This task does not have a receiver for the grouping " + groupingName);
    }

    return receiverMap.get(groupingName);
  }

  @Override
  public <K, V> ShuffleTupleSender<K, V> getSender(final String groupingName) {
    if (!senderMap.containsKey(groupingName)) {
      throw new RuntimeException("This task does not have a sender for the grouping " + groupingName);
    }

    return senderMap.get(groupingName);
  }

  @Override
  public <K, V> void registerLinkListener(final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    linkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  @Override
  public <K, V> void registerMessageHandler(final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    messageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == ImmutableShuffleMessageCode.TOPOLOGY_SETUP) {
        synchronized (ImmutableShuffleTopologyClient.this) {
          isTopologySetup = true;
          ImmutableShuffleTopologyClient.this.notifyAll();
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

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      messageHandlerMap.get(groupingName).onNext(message);
    }
  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      linkListenerMap.get(groupingName).onSuccess(message);
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      linkListenerMap.get(groupingName).onException(cause, remoteAddress, message);
    }
  }
}
