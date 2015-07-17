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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleLinkListener;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class BaseTupleSender<K, V> implements TupleSender<K, V> {

  private final String shuffleName;
  private final String groupingName;
  private final ShuffleClient shuffleClient;
  private final ShuffleTupleLinkListener globalTupleLinkListener;
  private final ConnectionFactory<ShuffleTupleMessage> tupleMessageConnectionFactory;
  private final IdentifierFactory idFactory;
  private final GroupingDescription<K, V> groupingDescription;
  private final GroupingStrategy<K> groupingStrategy;
  private final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator;

  @Inject
  public BaseTupleSender(
      final ShuffleClient shuffleClient,
      final ShuffleTupleLinkListener globalTupleLinkListener,
      final NetworkConnectionService networkConnectionService,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final GroupingDescription<K, V> groupingDescription,
      final GroupingStrategy<K> groupingStrategy,
      final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator) {
    this.shuffleName = shuffleClient.getShuffleDescription().getShuffleName();
    this.groupingName = groupingDescription.getGroupingName();
    this.shuffleClient = shuffleClient;
    this.globalTupleLinkListener = globalTupleLinkListener;
    this.tupleMessageConnectionFactory = networkConnectionService
        .getConnectionFactory(idFactory.getNewInstance(ShuffleParameters.NETWORK_CONNECTION_SERVICE_ID));
    this.idFactory = idFactory;
    this.groupingDescription = groupingDescription;
    this.groupingStrategy = groupingStrategy;
    this.tupleMessageGenerator = tupleMessageGenerator;
  }

  @Override
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createClassifiedTupleMessageList(tuple));
  }

  @Override
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createClassifiedTupleMessageList(tupleList));
  }

  @Override
  public void sendTupleTo(final String destNodeId, final Tuple<K, V> tuple) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(destNodeId, tupleMessageGenerator.createTupleMessage(tuple)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
  public void sendTupleTo(final String destNodeId, final List<Tuple<K, V>> tupleList) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(destNodeId, tupleMessageGenerator.createTupleMessage(tupleList)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
  public void registerTupleLinkListener(final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    globalTupleLinkListener.registerLinkListener(shuffleName, groupingName, linkListener);
  }

  private List<String> sendShuffleMessageTupleList(
      final List<Tuple<String, ShuffleTupleMessage<K, V>>> messageTupleList) {
    final List<String> receiverList = new ArrayList<>(messageTupleList.size());
    for (final Tuple<String, ShuffleTupleMessage<K, V>> shuffleMessageTuple : messageTupleList) {
      sendShuffleMessageTuple(shuffleMessageTuple);
      receiverList.add(shuffleMessageTuple.getKey());
    }

    return receiverList;
  }

  private void sendShuffleMessageTuple(final Tuple<String, ShuffleTupleMessage<K, V>> messageTuple) {
    try {
      final Connection<ShuffleTupleMessage> connection = tupleMessageConnectionFactory
          .newConnection(idFactory.getNewInstance(messageTuple.getKey()));
      connection.open();
      connection.write(messageTuple.getValue());
    } catch (final NetworkException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return groupingDescription;
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return groupingStrategy.selectReceivers(key,
        shuffleClient.getShuffleDescription().getReceiverIdList(groupingName));
  }
}
