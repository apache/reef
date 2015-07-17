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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriverConfiguration;
import org.apache.reef.io.network.shuffle.network.*;
import org.apache.reef.io.network.shuffle.params.SerializedShuffleSet;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class ShuffleServiceImpl implements ShuffleService {

  private final Injector rootInjector;
  private final ConnectionFactory<ShuffleControlMessage> controlMessageConnectionFactory;
  private final IdentifierFactory idFactory;
  private final Identifier driverIdentifier;
  private final Identifier taskIdentifier;
  private final ConfigurationSerializer confSerializer;
  private final ShuffleControlMessageHandler shuffleControlMessageHandler;
  private final ShuffleControlLinkListener shuffleControlLinkListener;
  private final ShuffleTupleMessageHandler shuffleTupleMessageHandler;
  private final ShuffleTupleLinkListener shuffleTupleLinkListener;

  private final Map<String, ShuffleClient> clientMap;
  private final Map<String, ShuffleClientMessageDispatcher> messageDispatcherMap;
  private final GlobalTupleCodecMap globalTupleCodecMap;

  @Inject
  public ShuffleServiceImpl(
      final Injector rootInjector,
      final NetworkConnectionService networkConnectionService,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final @Parameter(SerializedShuffleSet.class) Set<String> serializedTopologySet,
      final ConfigurationSerializer confSerializer,
      final ShuffleControlMessageHandler shuffleControlMessageHandler,
      final ShuffleControlLinkListener shuffleControlLinkListener,
      final ShuffleTupleMessageHandler shuffleTupleMessageHandler,
      final ShuffleTupleLinkListener shuffleTupleLinkListener,
      final GlobalTupleCodecMap globalTupleCodecMap) {

    this.rootInjector = rootInjector;
    this.controlMessageConnectionFactory = networkConnectionService
        .getConnectionFactory(idFactory.getNewInstance(ShuffleNetworkConnectionId.CONTROL_MESSAGE));
    this.idFactory = idFactory;
    this.driverIdentifier = idFactory.getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_IDENTIFIER);
    this.taskIdentifier = idFactory.getNewInstance(taskId);
    this.confSerializer = confSerializer;
    this.clientMap = new ConcurrentHashMap<>();
    this.messageDispatcherMap = new ConcurrentHashMap<>();
    this.globalTupleCodecMap = globalTupleCodecMap;
    this.shuffleControlLinkListener = shuffleControlLinkListener;
    this.shuffleControlMessageHandler = shuffleControlMessageHandler;
    this.shuffleTupleMessageHandler = shuffleTupleMessageHandler;
    this.shuffleTupleLinkListener = shuffleTupleLinkListener;
    deserializeClients(serializedTopologySet);
  }

  private void deserializeClients(final Set<String> serializedTopologySet) {
    for (final String serializedTopology : serializedTopologySet) {
      deserializeClient(serializedTopology);
    }
  }

  private void deserializeClient(final String serializedTopology) {
    try {
      final Configuration topologyConfig = confSerializer.fromString(serializedTopology);
      final Injector injector = rootInjector.forkInjector(topologyConfig);
      injector.bindVolatileInstance(ShuffleService.class, this);
      final ShuffleClient client = injector.getInstance(ShuffleClient.class);
      final ShuffleDescription description = client.getShuffleDescription();
      final ShuffleClientMessageDispatcher messageDispatcher = new ShuffleClientMessageDispatcher(client);
      clientMap.put(description.getShuffleName().getName(), client);
      registTupleCodecs(client);
      messageDispatcherMap.put(description.getShuffleName().getName(), messageDispatcher);
      shuffleControlLinkListener.registerLinkListener(description.getShuffleName(), messageDispatcher.getControlLinkListener());
      shuffleControlMessageHandler.registerMessageHandler(description.getShuffleName(), messageDispatcher.getControlMessageHandler());
      shuffleTupleLinkListener.registerLinkListener(description.getShuffleName(), messageDispatcher.getTupleLinkListener());
      shuffleTupleMessageHandler.registerMessageHandler(description.getShuffleName(), messageDispatcher.getTupleMessageHandler());
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing topology " + serializedTopology, exception);
    }
  }

  private void registTupleCodecs(final ShuffleClient client) {
    final ShuffleDescription description = client.getShuffleDescription();
    for (final String groupingName : description.getGroupingNameList()) {
      globalTupleCodecMap.registerTupleCodec(description.getShuffleName().getName(), groupingName, client.getTupleCodec(groupingName));
    }
  }

  @Override
  public ShuffleClient getClient(final Class<? extends Name<String>> clientName) {
    return clientMap.get(clientName.getName());
  }

  @Override
  public ShuffleClient getClient(final String topologyName) {
    return clientMap.get(topologyName);
  }

  @Override
  public void sendControlMessage(String destId, int code, String shuffleName, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    final ShuffleControlMessage controlMessage = new ShuffleControlMessage(code, shuffleName, groupingName, data, sourceType, sinkType);
    final Identifier destIdentifier = idFactory.getNewInstance(destId);
    try {
      final Connection<ShuffleControlMessage> connection = controlMessageConnectionFactory.newConnection(destIdentifier);
      connection.open();
      connection.write(controlMessage);
    } catch (final NetworkException e) {
      messageDispatcherMap.get(shuffleName).getControlLinkListener()
          .onException(e, null, createNetworkShuffleControlMessage(destIdentifier, controlMessage));
    }
  }

  @Override
  public void sendControlMessageToDriver(int code, String shuffleName, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    final ShuffleControlMessage controlMessage = new ShuffleControlMessage(code, shuffleName, groupingName, data, sourceType, sinkType);
    try {
      final Connection<ShuffleControlMessage> connection = controlMessageConnectionFactory.newConnection(driverIdentifier);
      connection.open();
      connection.write(controlMessage);
    } catch (final NetworkException e) {
      messageDispatcherMap.get(shuffleName).getControlLinkListener()
          .onException(e, null, createNetworkShuffleControlMessage(driverIdentifier, controlMessage));
    }
  }

  private Message<ShuffleControlMessage> createNetworkShuffleControlMessage(
      final Identifier destIdentifier, final ShuffleControlMessage controlMessage) {
    return new NSMessage<>(taskIdentifier, destIdentifier, controlMessage);
  }



  @Override
  public <K, V> void registerTupleLinkListener(String shuffleName, String groupingName, LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    messageDispatcherMap.get(shuffleName)
        .registerTupleLinkListener(groupingName, linkListener);
  }

  @Override
  public <K, V> void registerTupleMessageHandler(String shuffleName, String groupingName, EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    messageDispatcherMap.get(shuffleName)
        .registerTupleMessageHandler(groupingName, messageHandler);
  }

  @Override
  public void registerSenderGroupingController(String shuffleName, GroupingController groupingController) {
    final String groupingName = groupingController.getGroupingDescription().getGroupingName();
    final ShuffleClientMessageDispatcher dispatcher = messageDispatcherMap.get(shuffleName);
    dispatcher.registerSenderControlMessageHandler(groupingName, groupingController);
    dispatcher.registerSenderControlLinkListener(groupingName, groupingController);
  }

  @Override
  public void registerReceiverGroupingController(String shuffleName, GroupingController groupingController) {
    final String groupingName = groupingController.getGroupingDescription().getGroupingName();
    final ShuffleClientMessageDispatcher dispatcher = messageDispatcherMap.get(shuffleName);
    dispatcher.registerReceiverControlMessageHandler(groupingName, groupingController);
    dispatcher.registerReceiverControlLinkListener(groupingName, groupingController);
  }
}
