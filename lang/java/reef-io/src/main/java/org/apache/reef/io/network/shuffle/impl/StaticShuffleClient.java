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
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.task.operator.TupleOperatorFactory;
import org.apache.reef.io.network.shuffle.task.operator.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.operator.TupleSender;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public final class StaticShuffleClient implements ShuffleClient {

  private final ShuffleService shuffleService;
  private final String shuffleName;
  private final ShuffleDescription initialShuffleDescription;
  private final ClientTupleCodecMap tupleCodecMap;
  private final TupleOperatorFactory tupleOperatorFactory;

  private final CountDownLatch clientSetupLatch;
  private final Map<String, CountDownLatch> groupingSetupLatchMap;

  @Inject
  public StaticShuffleClient(
      final ShuffleService shuffleService,
      final ShuffleDescription initialShuffleDescription,
      final ClientTupleCodecMap tupleCodecMap,
      final TupleOperatorFactory tupleOperatorFactory) {
    this.shuffleService = shuffleService;
    this.shuffleName = initialShuffleDescription.getShuffleName().getName();
    this.initialShuffleDescription = initialShuffleDescription;
    this.tupleOperatorFactory = tupleOperatorFactory;
    this.tupleCodecMap = tupleCodecMap;

    this.groupingSetupLatchMap = new ConcurrentHashMap<>();
    for (final String groupingName : initialShuffleDescription.getGroupingNameList()) {
      groupingSetupLatchMap.put(groupingName, new CountDownLatch(1));
    }

    this.clientSetupLatch = new CountDownLatch(groupingSetupLatchMap.size());
  }

  @Override
  public ShuffleDescription getShuffleDescription() {
    return initialShuffleDescription;
  }

  @Override
  public void waitForGroupingSetup(final String groupingName) {
    try {
      groupingSetupLatchMap.get(groupingName).await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void waitForSetup() {
    try {
      clientSetupLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void groupingInitialized(final String groupingName) {
    if (groupingSetupLatchMap.get(groupingName).getCount() != 0) {
      groupingSetupLatchMap.get(groupingName).countDown();
      clientSetupLatch.countDown();
    }
  }

  private void onDriverMessage(final ShuffleControlMessage driverMessage) {
    if (driverMessage.getCode() == StaticShuffleMessageCode.GROUPING_SETUP) {
      groupingInitialized(driverMessage.getGroupingName());
    }
  }

  @Override
  public Codec<Tuple> getTupleCodec(final String groupingName) {
    return tupleCodecMap.getTupleCodec(groupingName);
  }

  @Override
  public <K, V> TupleReceiver<K, V> getReceiver(final String groupingName) {
    return tupleOperatorFactory.newTupleReceiver(initialShuffleDescription.getGroupingDescription(groupingName));
  }

  @Override
  public <K, V> TupleSender<K, V> getSender(final String groupingName) {
    return tupleOperatorFactory.newTupleSender(initialShuffleDescription.getGroupingDescription(groupingName));
  }

  @Override
  public void sendControlMessage(String destId, int code, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    shuffleService.sendControlMessage(destId, code, shuffleName, groupingName, data, sourceType, sinkType);
  }

  @Override
  public void sendControlMessageToDriver(int code, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    shuffleService.sendControlMessageToDriver(code, shuffleName, groupingName, data, sourceType, sinkType);
  }

  @Override
  public <K, V> void registerTupleLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    shuffleService.registerTupleLinkListener(shuffleName, groupingName, linkListener);
  }

  @Override
  public <K, V> void registerTupleMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    shuffleService.registerTupleMessageHandler(shuffleName, groupingName, messageHandler);
  }

  @Override
  public void registerSenderGroupingController(GroupingController groupingController) {
    shuffleService.registerSenderGroupingController(shuffleName, groupingController);
  }

  @Override
  public void registerReceiverGroupingController(GroupingController groupingController) {
    shuffleService.registerReceiverGroupingController(shuffleName, groupingController);
  }

  @Override
  public void onNext(Message<ShuffleControlMessage> value) {
    onDriverMessage(value.getData().iterator().next());
  }

  @Override
  public void onSuccess(Message<ShuffleControlMessage> message) {
  }

  @Override
  public void onException(Throwable cause, SocketAddress remoteAddress, Message<ShuffleControlMessage> message) {
  }
}
