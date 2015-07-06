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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.grouping.Grouping;
import org.apache.reef.io.network.shuffle.ns.ShuffleMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.params.ShuffleTopologyName;
import org.apache.reef.io.network.shuffle.params.ShuffleTupleCodec;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
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

  private final String topologyName;
  private final GroupingDescription<K, V> groupingDescription;
  private final NodePoolDescription receiverPoolDescription;
  private final Grouping<K> grouping;
  private final StreamingCodec<Tuple<K, V>> tupleCodec;

  @Inject
  public ShuffleTupleSerializerImpl(
      final @Parameter(ShuffleTopologyName.class) String topologyName,
      final @Parameter(ShuffleTupleCodec.class) StreamingCodec<Tuple<K, V>> tupleCodec,
      final NodePoolDescription receiverPoolDescription,
      final GroupingDescription<K, V> groupingDescription,
      final Grouping<K> grouping) {
    this.topologyName = topologyName;
    this.tupleCodec = tupleCodec;
    this.groupingDescription = groupingDescription;
    this.receiverPoolDescription = receiverPoolDescription;
    this.grouping = grouping;
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
    final List<String> nodeIdList = grouping.selectReceivers(key, receiverPoolDescription);
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
      for (final String nodeId : grouping.selectReceivers(tuple.getKey(), receiverPoolDescription)) {
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
    return new ShuffleTupleMessage<>(ShuffleMessage.TUPLE_MESSAGE, topologyName, groupingDescription.getGroupingName(), data);
  }
}
