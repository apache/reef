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

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.descriptor.GroupingDescriptor;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class BaseTupleReceiver<K, V> implements TupleReceiver<K, V> {

  private final ShuffleClient shuffleClient;
  private final GroupingDescriptor<K, V> groupingDescriptor;
  private final GroupingStrategy<K> groupingStrategy;

  @Inject
  public BaseTupleReceiver(
      final ShuffleClient shuffleClient,
      final GroupingDescriptor<K, V> groupingDescriptor,
      final GroupingStrategy<K> groupingStrategy) {
    this.groupingDescriptor = groupingDescriptor;
    this.shuffleClient = shuffleClient;
    this.groupingStrategy = groupingStrategy;
  }

  @Override
  public void registerMessageHandler(final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    shuffleClient.registerMessageHandler(getGroupingName(), messageHandler);
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
}
