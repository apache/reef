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
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public class SynchronizedTupleReceiver<K, V> implements TupleReceiver<K, V> {

  private final TupleReceiver<K, V> baseReceiver;
  private final Queue<Tuple<K, V>> receivedTupleQueue;

  @Inject
  public SynchronizedTupleReceiver(TupleReceiver<K, V> baseReceiver) {
    this.baseReceiver = baseReceiver;
    baseReceiver.registerTupleMessageHandler(new ReceiverMessageHandler());
    baseReceiver.registerGroupingController(new ReceiverGroupingController());
    receivedTupleQueue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void registerTupleMessageHandler(EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    baseReceiver.registerTupleMessageHandler(messageHandler);
  }

  @Override
  public GroupingDescription<K, V> getGroupingDescription() {
    return baseReceiver.getGroupingDescription();
  }

  @Override
  public List<String> getSelectedReceiverIdList(K key) {
    return baseReceiver.getSelectedReceiverIdList(key);
  }

  @Override
  public void waitForGroupingSetup() {
    baseReceiver.waitForGroupingSetup();
  }

  @Override
  public void registerGroupingController(GroupingController groupingController) {
    baseReceiver.registerGroupingController(groupingController);
  }

  @Override
  public void sendControlMessage(String destId, int code, byte[][] data, byte type) {
    baseReceiver.sendControlMessage(destId, code, data, type);
  }

  @Override
  public void sendControlMessageToDriver(int code, byte[][] data, byte type) {
    baseReceiver.sendControlMessageToDriver(code, data, type);
  }

  public Collection<Tuple<K, V>> receiveTuples() {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {

    }
    return receivedTupleQueue;
  }

  private class ReceiverMessageHandler implements EventHandler<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onNext(Message<ShuffleTupleMessage<K, V>> value) {
      for (final ShuffleTupleMessage<K, V> tupleMessage : value.getData()) {
        for (int i = 0; i < tupleMessage.size(); i++) {
          receivedTupleQueue.add(tupleMessage.get(i));
        }
      }
    }
  }

  private class ReceiverGroupingController implements GroupingController {

    @Override
    public GroupingDescription getGroupingDescription() {
      return baseReceiver.getGroupingDescription();
    }

    @Override
    public void onNext(Message<ShuffleControlMessage> value) {

    }

    @Override
    public void onSuccess(Message<ShuffleControlMessage> message) {

    }

    @Override
    public void onException(Throwable cause, SocketAddress remoteAddress, Message<ShuffleControlMessage> message) {

    }
  }
}
