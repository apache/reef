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
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class BaseTupleSender<K, V> implements ShuffleTupleSender<K, V> {

  private final ShuffleTopologyClient topologyClient;
  private final ConnectionFactory<ShuffleTupleMessage> connFactory;
  private final Map<String, Connection<ShuffleTupleMessage>> connMap;
  private final IdentifierFactory idFactory;
  private final NodePoolDescription receiverPool;
  private final GroupingDescription<K, V> groupingDescription;
  private final ShuffleTupleSerializer<K, V> tupleSerializer;

  @Inject
  public BaseTupleSender(
      final ShuffleTopologyClient topologyClient,
      final ConnectionFactory<ShuffleTupleMessage> connFactory,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final NodePoolDescription receiverPool,
      final GroupingDescription<K, V> groupingDescription,
      final ShuffleTupleSerializer<K, V> tupleSerializer) {

    this.topologyClient = topologyClient;
    this.connFactory = connFactory;
    this.idFactory = idFactory;
    this.connMap = new HashMap<>();
    this.receiverPool = receiverPool;
    this.groupingDescription = groupingDescription;
    this.tupleSerializer = tupleSerializer;
    createConnections();
  }

  private void createConnections() {
    for (final String nodeId : receiverPool.getNodeIdList()) {
      createConnection(nodeId);
    }
  }

  private void createConnection(final String nodeId) {
    final Connection<ShuffleTupleMessage> connection = connFactory.newConnection(idFactory.getNewInstance(nodeId));
    connMap.put(nodeId, connection);
  }

  @Override
  public int sendTuple(final Tuple<K, V> tuple) {
    return sendShuffleMessageTupleList(tupleSerializer.serializeTuple(tuple));
  }

  @Override
  public int sendTuple(final List<Tuple<K, V>> tupleList) {
    return sendShuffleMessageTupleList(tupleSerializer.serializeTupleList(tupleList));
  }

  @Override
  public int sendTuple(final K key, final List<V> valueList) {
    return sendShuffleMessageTupleList(tupleSerializer.serializeTuple(key, valueList));
  }

  @Override
  public void registerLinkListener(final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    topologyClient.registerLinkListener(getGroupingName(), linkListener);
  }

  private int sendShuffleMessageTupleList(final List<Tuple<String, ShuffleTupleMessage>> messageTupleList) {
    for (final Tuple<String, ShuffleTupleMessage> shuffleMessageTuple : messageTupleList) {
      sendShuffleMessageTuple(shuffleMessageTuple);
    }

    return messageTupleList.size();
  }

  private void sendShuffleMessageTuple(final Tuple<String, ShuffleTupleMessage> messageTuple) {
    topologyClient.waitForTopologySetup();

    try {
      connMap.get(messageTuple.getKey()).open();
      connMap.get(messageTuple.getKey()).write(messageTuple.getValue());
    } catch (NetworkException e) {
      // TODO : This unnecessary try-catch clause should be removed by changing API of Link interface.
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getGroupingName() {
    return groupingDescription.getGroupingName();
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return groupingDescription;
  }
}
