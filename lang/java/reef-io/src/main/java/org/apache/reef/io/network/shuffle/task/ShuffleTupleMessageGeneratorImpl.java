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

import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;

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
  private final String shuffleName;
  private final GroupingDescription<K, V> groupingDescription;
  private final GroupingStrategy<K> groupingStrategy;

  @Inject
  public ShuffleTupleMessageGeneratorImpl(
      final ShuffleClient shuffleClient,
      final GroupingDescription<K, V> groupingDescription,
      final GroupingStrategy<K> groupingStrategy) {
    this.shuffleClient = shuffleClient;
    this.shuffleName = shuffleClient.getShuffleDescription().getShuffleName().getName();
    this.groupingDescription = groupingDescription;
    this.groupingStrategy = groupingStrategy;
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(final Tuple<K, V> tuple) {
    return serializeTupleWithData(tuple.getKey(), new Tuple[] { tuple });
  }

  private List<Tuple<String, ShuffleTupleMessage<K, V>>> serializeTupleWithData(final K key, final Tuple<K, V>[] data) {
    final List<String> nodeIdList = groupingStrategy.selectReceivers(key, getReceiverIdList());
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
  public List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(final List<Tuple<K, V>> tupleList) {
    final Map<String, List<Tuple>> serializedTupleDataMap = new HashMap<>();
    for (final Tuple<K, V> tuple : tupleList) {
      for (final String nodeId : groupingStrategy.selectReceivers(tuple.getKey(), getReceiverIdList())) {
        if (!serializedTupleDataMap.containsKey(nodeId)) {
          serializedTupleDataMap.put(nodeId, new ArrayList<Tuple>());
        }
        serializedTupleDataMap.get(nodeId).add(tuple);
      }
    }

    final List<Tuple<String, ShuffleTupleMessage<K, V>>> serializedTupleList = new ArrayList<>(serializedTupleDataMap.size());
    for (Map.Entry<String, List<Tuple>> entry : serializedTupleDataMap.entrySet()) {
      int i = 0;
      final Tuple[] data = new Tuple[entry.getValue().size()];
      for (final Tuple tuple : entry.getValue()) {
        data[i++] = tuple;
      }

      serializedTupleList.add(new Tuple<>(entry.getKey(), createShuffleTupleMessage(data)));
    }

    return serializedTupleList;
  }

  @Override
  public ShuffleTupleMessage<K, V> createTupleMessage(final Tuple<K, V> tuple) {
    return createShuffleTupleMessage(new Tuple[]{tuple });
  }

  @Override
  public ShuffleTupleMessage<K, V> createTupleMessage(final List<Tuple<K, V>> tupleList) {
    return createShuffleTupleMessage((Tuple[]) tupleList.toArray());
  }

  private List<String> getReceiverIdList() {
    return shuffleClient.getShuffleDescription().getReceiverIdList(groupingDescription.getGroupingName());
  }

  private ShuffleTupleMessage<K, V> createShuffleTupleMessage(final Tuple<K, V>[] data) {
    return new ShuffleTupleMessage<>(shuffleName, groupingDescription.getGroupingName(), data);
  }
}
