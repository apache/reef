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
import org.apache.reef.io.network.shuffle.params.ReceiverIdList;
import org.apache.reef.io.network.shuffle.params.SerializedShuffleName;
import org.apache.reef.io.network.shuffle.topology.GroupingDescriptor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class ShuffleTupleSerializerImpl<K, V> implements ShuffleTupleSerializer<K, V> {

  private final String shuffleName;
  private final GroupingDescriptor<K, V> groupingDescription;
  private final List<String> receiverIdList;
  private final GroupingStrategy<K> groupingStrategy;

  @Inject
  public ShuffleTupleSerializerImpl(
      final @Parameter(SerializedShuffleName.class) String shuffleName,
      final @Parameter(ReceiverIdList.class) List<String> receiverIdList,
      final GroupingDescriptor<K, V> groupingDescription,
      final GroupingStrategy<K> groupingStrategy) {
    this.shuffleName = shuffleName;
    this.groupingDescription = groupingDescription;
    this.receiverIdList = receiverIdList;
    this.groupingStrategy = groupingStrategy;
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage>> serializeTuple(final Tuple<K, V> tuple) {
    return serializeTupleWithData(tuple.getKey(), new Tuple[] { tuple });
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage>> serializeTuple(final K key, final List<V> valueList) {
    final Tuple[] tupleArr = new Tuple[valueList.size()];

    int i = 0;
    for (final V value : valueList) {
      tupleArr[i++] = new Tuple(key, value);
    }

    return serializeTupleWithData(key, tupleArr);
  }

  private List<Tuple<String, ShuffleTupleMessage>> serializeTupleWithData(final K key, final Tuple<K, V>[] data) {
    final List<String> nodeIdList = groupingStrategy.selectReceivers(key, receiverIdList);
    final List<Tuple<String, ShuffleTupleMessage>> messageList = new ArrayList<>(nodeIdList.size());
    for (final String nodeId : nodeIdList) {
      messageList.add(new Tuple<>(
          nodeId,
          createShuffleMessage(data)
      ));
    }

    return messageList;
  }

  @Override
  public List<Tuple<String, ShuffleTupleMessage>> serializeTupleList(final List<Tuple<K, V>> tupleList) {
    final Map<String, List<Tuple>> serializedTupleDataMap = new HashMap<>();
    for (final Tuple<K, V> tuple : tupleList) {
      for (final String nodeId : groupingStrategy.selectReceivers(tuple.getKey(), receiverIdList)) {
        if (!serializedTupleDataMap.containsKey(nodeId)) {
          serializedTupleDataMap.put(nodeId, new ArrayList<Tuple>());
        }
        serializedTupleDataMap.get(nodeId).add(tuple);
      }
    }

    final List<Tuple<String, ShuffleTupleMessage>> serializedTupleList = new ArrayList<>(serializedTupleDataMap.size());
    for (Map.Entry<String, List<Tuple>> entry : serializedTupleDataMap.entrySet()) {
      int i = 0;
      final Tuple[] data = new Tuple[entry.getValue().size()];
      for (final Tuple tuple : entry.getValue()) {
        data[i++] = tuple;
      }

      serializedTupleList.add(new Tuple<>(entry.getKey(), createShuffleMessage(data)));
    }

    return serializedTupleList;
  }

  private ShuffleTupleMessage createShuffleMessage(final Tuple<K, V>[] data) {
    return new ShuffleTupleMessage<>(shuffleName, groupingDescription.getGroupingName(), data);
  }
}
