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
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.descriptor.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleMessageNSId;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class BaseTupleSender<K, V> implements TupleSender<K, V> {

  private final ShuffleClient shuffleClient;
  private final ConnectionFactory<ShuffleTupleMessage> connFactory;
  private final Map<String, Connection<ShuffleTupleMessage>> connMap;
  private final IdentifierFactory idFactory;
  private final Identifier taskId;
  private final GroupingDescriptor<K, V> groupingDescriptor;
  private final GroupingStrategy<K> groupingStrategy;
  private final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator;

  @Inject
  public BaseTupleSender(
      final ShuffleClient shuffleClient,
      final NetworkServiceClient nsClient,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
      final GroupingDescriptor<K, V> groupingDescriptor,
      final GroupingStrategy<K> groupingStrategy,
      final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator) {
    this.shuffleClient = shuffleClient;
    this.connFactory = nsClient.getConnectionFactory(ShuffleTupleMessageNSId.class);
    this.idFactory = idFactory;
    this.taskId = idFactory.getNewInstance(taskId);
    this.connMap = new HashMap<>();
    this.groupingDescriptor = groupingDescriptor;
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
  public void registerLinkListener(final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    shuffleClient.registerLinkListener(getGroupingName(), linkListener);
  }

  private int sendShuffleMessageTupleList(final List<Tuple<String, ShuffleTupleMessage<K, V>>> messageTupleList) {
    for (final Tuple<String, ShuffleTupleMessage<K, V>> shuffleMessageTuple : messageTupleList) {
      sendShuffleMessageTuple(shuffleMessageTuple);
    }

    return messageTupleList.size();
  }

  private void sendShuffleMessageTuple(final Tuple<String, ShuffleTupleMessage<K, V>> messageTuple) {
    shuffleClient.waitForGroupingSetup(groupingDescriptor.getGroupingName());

    if (!connMap.containsKey(messageTuple.getKey())) {
      try {
        final Connection<ShuffleTupleMessage> connection = connFactory.newConnection(idFactory.getNewInstance(messageTuple.getKey()));
        connection.open();
        connMap.put(messageTuple.getKey(), connection);
        connection.write(messageTuple.getValue());
      } catch (final NetworkException exception) {
        shuffleClient.getTupleLinkListener().onException(
            exception, null, createShuffleTupleNetworkMessage(messageTuple.getKey(), messageTuple.getValue()));
      }
    } else {
      connMap.get(messageTuple.getKey()).write(messageTuple.getValue());
    }
  }

  private Message<ShuffleTupleMessage> createShuffleTupleNetworkMessage(final String destId, final ShuffleTupleMessage message) {
    return new NSMessage<>(taskId, idFactory.getNewInstance(destId), message);
  }

  @Override
  public String getGroupingName() {
    return groupingDescriptor.getGroupingName();
  }

  @Override
  public GroupingDescriptor<K, V> getGroupingDescriptor() {
    return groupingDescriptor;
  }

  @Override
  public GroupingStrategy<K> getGroupingStrategy() {
    return groupingStrategy;
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return groupingStrategy.selectReceivers(key,
        shuffleClient.getShuffleDescriptor().getReceiverIdList(groupingDescriptor.getGroupingName()));
  }

  @Override
  public void waitForGroupingSetup() {
    shuffleClient.waitForGroupingSetup(groupingDescriptor.getGroupingName());
  }
}
