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
import org.apache.reef.io.network.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessageHandler;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class BaseTupleReceiver<K, V> implements TupleReceiver<K, V> {

  private final String shuffleGroupName;
  private final String shuffleName;
  private final ShuffleClient shuffleClient;
  private final ShuffleDescription<K, V> shuffleDescription;
  private final ShuffleStrategy<K> shuffleStrategy;
  private final ShuffleTupleMessageHandler globalTupleMessageHandler;

  @Inject
  public BaseTupleReceiver(
      final ShuffleClient shuffleClient,
      final ShuffleDescription<K, V> shuffleDescription,
      final ShuffleStrategy<K> shuffleStrategy,
      final ShuffleTupleMessageHandler globalTupleMessageHandler) {
    this.shuffleGroupName = shuffleClient.getShuffleGroupDescription().getShuffleGroupName();
    this.shuffleName = shuffleDescription.getShuffleName();
    this.shuffleDescription = shuffleDescription;
    this.shuffleClient = shuffleClient;
    this.shuffleStrategy = shuffleStrategy;
    this.globalTupleMessageHandler = globalTupleMessageHandler;
  }

  @Override
  public void registerTupleMessageHandler(final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    globalTupleMessageHandler.registerMessageHandler(shuffleGroupName, shuffleName, messageHandler);
  }

  @Override
  public ShuffleDescription<K, V> getShuffleDescription() {
    return shuffleDescription;
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return shuffleStrategy.selectReceivers(key,
        shuffleClient.getShuffleGroupDescription().getReceiverIdList(shuffleDescription.getShuffleName()));
  }
}
