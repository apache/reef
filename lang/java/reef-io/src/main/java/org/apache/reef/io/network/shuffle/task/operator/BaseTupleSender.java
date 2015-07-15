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
package org.apache.reef.io.network.shuffle.task.operator;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleNetworkConnectionId;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.task.ShuffleTupleMessageGenerator;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class BaseTupleSender<K, V> implements TupleSender<K, V> {

  private final String shuffleName;
  private final String groupingName;
  private final ShuffleClient shuffleClient;
  private final ConnectionFactory<ShuffleTupleMessage> tupleMessageConnectionFactory;
  private final ConnectionFactory<ShuffleControlMessage> controlMessageConnectionFactory;
  private final Map<String, Connection<ShuffleTupleMessage>> tupleMessageConnectionMap;
  private final Map<String, Connection<ShuffleControlMessage>> controlMessageConnectionMap;
  private final IdentifierFactory idFactory;
  private final Identifier taskId;
  private final GroupingDescription<K, V> groupingDescription;
  private final GroupingStrategy<K> groupingStrategy;
  private final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator;

  @Inject
  public BaseTupleSender(
      final ShuffleClient shuffleClient,
      final NetworkConnectionService networkConnectionService,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final GroupingDescription<K, V> groupingDescription,
      final GroupingStrategy<K> groupingStrategy,
      final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator) {
    this.shuffleName = shuffleClient.getShuffleDescription().getShuffleName().getName();
    this.groupingName = groupingDescription.getGroupingName();
    this.shuffleClient = shuffleClient;
    this.tupleMessageConnectionFactory = networkConnectionService
        .getConnectionFactory(idFactory.getNewInstance(ShuffleNetworkConnectionId.TUPLE_MESSAGE));
    this.controlMessageConnectionFactory = networkConnectionService
        .getConnectionFactory(idFactory.getNewInstance(ShuffleNetworkConnectionId.CONTROL_MESSAGE));
    this.idFactory = idFactory;
    this.taskId = idFactory.getNewInstance(taskId);
    this.tupleMessageConnectionMap = new ConcurrentHashMap<>();
    this.controlMessageConnectionMap = new ConcurrentHashMap<>();
    this.groupingDescription = groupingDescription;
    this.groupingStrategy = groupingStrategy;
    this.tupleMessageGenerator = tupleMessageGenerator;
  }

  @Override
  public int sendTuple(final Tuple<K, V> tuple) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createClassifiedTupleMessageList(tuple));
  }

  @Override
  public int sendTuple(final List<Tuple<K, V>> tupleList) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createClassifiedTupleMessageList(tupleList));
  }

  @Override
  public int sendTupleTo(final String destNodeId, final Tuple<K, V> tuple) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(destNodeId, tupleMessageGenerator.createTupleMessage(tuple)));
    return sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
  public int sendTupleTo(final String destNodeId, final List<Tuple<K, V>> tupleList) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(destNodeId, tupleMessageGenerator.createTupleMessage(tupleList)));
    return sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
  public void registerTupleLinkListener(final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    shuffleClient.registerTupleLinkListener(groupingName, linkListener);
  }

  private int sendShuffleMessageTupleList(final List<Tuple<String, ShuffleTupleMessage<K, V>>> messageTupleList) {
    for (final Tuple<String, ShuffleTupleMessage<K, V>> shuffleMessageTuple : messageTupleList) {
      sendShuffleMessageTuple(shuffleMessageTuple);
    }

    return messageTupleList.size();
  }

  private void sendShuffleMessageTuple(final Tuple<String, ShuffleTupleMessage<K, V>> messageTuple) {
    shuffleClient.waitForGroupingSetup(groupingName);

    if (!tupleMessageConnectionMap.containsKey(messageTuple.getKey())) {
      try {
        final Connection<ShuffleTupleMessage> connection = tupleMessageConnectionFactory
            .newConnection(idFactory.getNewInstance(messageTuple.getKey()));
        connection.open();
        tupleMessageConnectionMap.put(messageTuple.getKey(), connection);
        connection.write(messageTuple.getValue());
      } catch (final NetworkException exception) {
        shuffleClient.getTupleLinkListener().onException(
            exception, null, createShuffleTupleNetworkMessage(messageTuple.getKey(), messageTuple.getValue()));
      }
    } else {
      tupleMessageConnectionMap.get(messageTuple.getKey()).write(messageTuple.getValue());
    }
  }

  private Message<ShuffleTupleMessage> createShuffleTupleNetworkMessage(final String destId, final ShuffleTupleMessage message) {
    return new NSMessage<>(taskId, idFactory.getNewInstance(destId), message);
  }

  private Message<ShuffleControlMessage> createShuffleControlNetworkMessage(final String destId, final ShuffleControlMessage message) {
    return new NSMessage<>(taskId, idFactory.getNewInstance(destId), message);
  }

  @Override
  public String getGroupingName() {
    return groupingName;
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return groupingDescription;
  }

  @Override
  public GroupingStrategy<K> getGroupingStrategy() {
    return groupingStrategy;
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return groupingStrategy.selectReceivers(key,
        shuffleClient.getShuffleDescription().getReceiverIdList(groupingName));
  }

  @Override
  public void waitForGroupingSetup() {
    shuffleClient.waitForGroupingSetup(groupingName);
  }

  @Override
  public void registerControlMessageHandler(final EventHandler<Message<ShuffleControlMessage>> messageHandler) {
    shuffleClient.registerControlMessageHandler(groupingName, messageHandler);
  }

  @Override
  public void registerControlLinkListener(final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    shuffleClient.registerControlLinkListener(groupingName, linkListener);
  }

  @Override
  public void sendControlMessage(final String destId, final int code, final byte[][] data) {
    final ShuffleControlMessage controlMessage = new ShuffleControlMessage(code, shuffleName,
        groupingName, data, false);

    shuffleClient.waitForGroupingSetup(groupingDescription.getGroupingName());

    if (!controlMessageConnectionMap.containsKey(destId)) {
      try {
        final Connection<ShuffleControlMessage> connection = controlMessageConnectionFactory
            .newConnection(idFactory.getNewInstance(destId));
        connection.open();
        controlMessageConnectionMap.put(destId, connection);
        connection.write(controlMessage);
      } catch (final NetworkException exception) {
        shuffleClient.getControlLinkListener().onException(
            exception, null, createShuffleControlNetworkMessage(destId, controlMessage));
      }
    } else {
      controlMessageConnectionMap.get(destId).write(controlMessage);
    }
  }
}
