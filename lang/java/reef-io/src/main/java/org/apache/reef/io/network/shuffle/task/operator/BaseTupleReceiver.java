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

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.grouping.GroupingStrategy;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class BaseTupleReceiver<K, V> implements TupleReceiver<K, V> {

  private final String groupingName;
  private final ShuffleClient shuffleClient;
  private final GroupingDescription<K, V> groupingDescription;
  private final GroupingStrategy<K> groupingStrategy;

  @Inject
  public BaseTupleReceiver(
      final ShuffleClient shuffleClient,
      final GroupingDescription<K, V> groupingDescription,
      final GroupingStrategy<K> groupingStrategy) {
    this.groupingName = groupingDescription.getGroupingName();
    this.groupingDescription = groupingDescription;
    this.shuffleClient = shuffleClient;
    this.groupingStrategy = groupingStrategy;
  }

  @Override
  public void registerTupleMessageHandler(final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    shuffleClient.registerTupleMessageHandler(groupingName, messageHandler);
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return groupingDescription;
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return groupingStrategy.selectReceivers(key,
        shuffleClient.getShuffleDescription().getReceiverIdList(groupingDescription.getGroupingName()));
  }

  @Override
  public void waitForGroupingSetup() {
    shuffleClient.waitForGroupingSetup(groupingDescription.getGroupingName());
  }

  @Override
  public void registerGroupingController(GroupingController groupingController) {
    shuffleClient.registerReceiverGroupingController(groupingController);
  }


  @Override
  public void sendControlMessage(String destId, int code, byte[][] data, byte sinkType) {
    shuffleClient.sendControlMessage(destId, code, groupingName, data, ShuffleControlMessage.RECEIVER, sinkType);
  }

  @Override
  public void sendControlMessageToDriver(int code, byte[][] data, byte sinkType) {
    shuffleClient.sendControlMessageToDriver(code, groupingName, data, ShuffleControlMessage.RECEIVER, sinkType);
  }
}
