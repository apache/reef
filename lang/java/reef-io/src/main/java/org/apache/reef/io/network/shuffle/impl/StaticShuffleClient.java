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
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.task.Tuple;
import org.apache.reef.io.network.shuffle.descriptor.ShuffleDescriptor;
import org.apache.reef.io.network.shuffle.utils.TupleMessageDispatcher;
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

  private final CountDownLatch clientSetupLatch;
  private final Map<String, CountDownLatch> groupingSetupLatchMap;

  private final ShuffleDescriptor initialShuffleDescriptor;
  private final ClientTupleCodecMap tupleCodecMap;
  private final TupleOperatorFactory tupleOperatorFactory;

  private final TupleMessageDispatcher tupleMessageDispatcher;
  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;

  @Inject
  public StaticShuffleClient(
      final ShuffleDescriptor initialShuffleDescriptor,
      final ClientTupleCodecMap tupleCodecMap,
      final TupleOperatorFactory tupleOperatorFactory) {

    this.initialShuffleDescriptor = initialShuffleDescriptor;
    this.tupleOperatorFactory = tupleOperatorFactory;
    this.tupleCodecMap = tupleCodecMap;
    this.tupleMessageDispatcher = new TupleMessageDispatcher();
    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
    this.groupingSetupLatchMap = new ConcurrentHashMap<>();
    for (final String groupingName : initialShuffleDescriptor.getGroupingNameList()) {
      groupingSetupLatchMap.put(groupingName, new CountDownLatch(1));
    }

    this.clientSetupLatch = new CountDownLatch(groupingSetupLatchMap.size());
  }

  @Override
  public ShuffleDescriptor getShuffleDescriptor() {
    return initialShuffleDescriptor;
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

  @Override
  public EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler() {
    return tupleMessageDispatcher.getTupleMessageHandler();
  }

  @Override
  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return tupleMessageDispatcher.getTupleLinkListener();
  }

  @Override
  public Codec<Tuple> getTupleCodec(String groupingName) {
    return tupleCodecMap.getTupleCodec(groupingName);
  }

  @Override
  public <K, V> TupleReceiver<K, V> createReceiver(final String groupingName) {
    return tupleOperatorFactory.newTupleReceiver(initialShuffleDescriptor.getGroupingDescriptor(groupingName));
  }

  @Override
  public <K, V, T extends TupleReceiver<K, V>> T createReceiver(
      final String groupingName, final Class<T> receiverClass) {
    return tupleOperatorFactory.newTupleReceiver(
        initialShuffleDescriptor.getGroupingDescriptor(groupingName), receiverClass);
  }

  @Override
  public <K, V> TupleSender<K, V> createSender(final String groupingName) {
    return tupleOperatorFactory.newTupleSender(initialShuffleDescriptor.getGroupingDescriptor(groupingName));
  }

  @Override
  public <K, V, T extends TupleSender<K, V>> T createSender(final String groupingName, final Class<T> senderClass) {
    return tupleOperatorFactory.newTupleSender(
        initialShuffleDescriptor.getGroupingDescriptor(groupingName), senderClass);
  }

  @Override
  public <K, V> void registerLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    tupleMessageDispatcher.registerLinkListener(groupingName, linkListener);
  }

  @Override
  public <K, V> void registerMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    tupleMessageDispatcher.registerMessageHandler(groupingName, messageHandler);
  }

  @Override
  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return controlMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return controlLinkListener;
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage shuffleMessage = message.getData().iterator().next();
      if (shuffleMessage.getCode() == StaticShuffleMessageCode.GROUPING_SETUP) {
        groupingInitialized(shuffleMessage.getGroupingName());
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {
    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
    }
  }
}
