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

import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class ShuffleTupleMessageGeneratorImpl<K, V> implements ShuffleTupleMessageGenerator<K, V> {

  private final ShuffleClient shuffleClient;
  private final String shuffleGroupName;
  private final ShuffleDescription<K, V> shuffleDescription;
  private final ShuffleStrategy<K> shuffleStrategy;

  @Inject
  public ShuffleTupleMessageGeneratorImpl(
      final ShuffleClient shuffleClient,
      final ShuffleDescription<K, V> shuffleDescription,
      final ShuffleStrategy<K> shuffleStrategy) {
    this.shuffleClient = shuffleClient;
    this.shuffleGroupName = shuffleClient.getShuffleGroupDescription().getShuffleGroupName();
    this.shuffleDescription = shuffleDescription;
    this.shuffleStrategy = shuffleStrategy;
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(final Tuple<K, V> tuple) {

    return serializeTupleWithData(tuple.getKey(), createSingleList(tuple));
  }

  private List<Tuple<String, ShuffleTupleMessage<K, V>>> serializeTupleWithData(
      final K key, final List<Tuple<K, V>> data) {
    final List<String> nodeIdList = shuffleStrategy.selectReceivers(key, getReceiverIdList());
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> messageList = new ArrayList<>(nodeIdList.size());
    for (final String nodeId : nodeIdList) {
      messageList.add(new Tuple<>(
          nodeId,
          createShuffleTupleMessage(data)
      ));
    }

    return messageList;
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(
      final List<Tuple<K, V>> tupleList) {
    final Map<String, List<Tuple>> serializedTupleDataMap = new HashMap<>();
    for (final Tuple<K, V> tuple : tupleList) {
      for (final String nodeId : shuffleStrategy.selectReceivers(tuple.getKey(), getReceiverIdList())) {
        if (!serializedTupleDataMap.containsKey(nodeId)) {
          serializedTupleDataMap.put(nodeId, new ArrayList<Tuple>());
        }
        serializedTupleDataMap.get(nodeId).add(tuple);
      }
    }

    final List<Tuple<String, ShuffleTupleMessage<K, V>>>
        serializedTupleList = new ArrayList<>(serializedTupleDataMap.size());
    for (Map.Entry<String, List<Tuple>> entry : serializedTupleDataMap.entrySet()) {
      final List<Tuple<K, V>> data = new ArrayList<>(entry.getValue().size());
      for (final Tuple tuple : entry.getValue()) {
        data.add(tuple);
      }

      serializedTupleList.add(new Tuple<>(entry.getKey(), createShuffleTupleMessage(data)));
    }

    return serializedTupleList;
  }

  @Override
  public ShuffleTupleMessage<K, V> createTupleMessage(final Tuple<K, V> tuple) {
    return createShuffleTupleMessage(createSingleList(tuple));
  }

  private List<Tuple<K, V>> createSingleList(final Tuple<K, V> tuple) {
    final List<Tuple<K, V>> list = new ArrayList<>(1);
    list.add(tuple);
    return list;
  }

  @Override
  public ShuffleTupleMessage<K, V> createTupleMessage(final List<Tuple<K, V>> tupleList) {
    return createShuffleTupleMessage(tupleList);
  }

  private List<String> getReceiverIdList() {
    return shuffleClient.getShuffleGroupDescription().getReceiverIdList(shuffleDescription.getShuffleName());
  }

  private ShuffleTupleMessage<K, V> createShuffleTupleMessage(final List<Tuple<K, V>> data) {
    return new ShuffleTupleMessage<>(shuffleGroupName, shuffleDescription.getShuffleName(), data);
  }
}
