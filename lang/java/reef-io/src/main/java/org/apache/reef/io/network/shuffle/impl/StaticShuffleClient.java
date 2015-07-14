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
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.task.operator.TupleOperatorFactory;
import org.apache.reef.io.network.shuffle.task.operator.TupleReceiver;
import org.apache.reef.io.network.shuffle.task.operator.TupleSender;
import org.apache.reef.io.network.shuffle.utils.ShuffleMessageDispatcher;
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

  private final ShuffleDescription initialShuffleDescription;
  private final ClientTupleCodecMap tupleCodecMap;
  private final TupleOperatorFactory tupleOperatorFactory;

  private final ShuffleMessageDispatcher shuffleMessageDispatcher;
  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;

  @Inject
  public StaticShuffleClient(
      final ShuffleDescription initialShuffleDescription,
      final ClientTupleCodecMap tupleCodecMap,
      final TupleOperatorFactory tupleOperatorFactory) {

    this.initialShuffleDescription = initialShuffleDescription;
    this.tupleOperatorFactory = tupleOperatorFactory;
    this.tupleCodecMap = tupleCodecMap;
    this.shuffleMessageDispatcher = new ShuffleMessageDispatcher();
    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
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
  public EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler() {
    return shuffleMessageDispatcher.getTupleMessageHandler();
  }

  @Override
  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return shuffleMessageDispatcher.getTupleLinkListener();
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
  public <K, V> void registerTupleLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    shuffleMessageDispatcher.registerTupleLinkListener(groupingName, linkListener);
  }

  @Override
  public <K, V> void registerTupleMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    shuffleMessageDispatcher.registerTupleMessageHandler(groupingName, messageHandler);
  }

  @Override
  public void registerControlLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    shuffleMessageDispatcher.registerControlLinkListener(groupingName, linkListener);
  }

  @Override
  public void registerControlMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleControlMessage>> messageHandler) {
    shuffleMessageDispatcher.registerControlMessageHandler(groupingName, messageHandler);
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
      if (shuffleMessage.isDriverMessage()) {
        onDriverMessage(shuffleMessage);
      } else {
        shuffleMessageDispatcher.getControlMessageHandler().onNext(message);
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage shuffleMessage = message.getData().iterator().next();
      if (!shuffleMessage.isDriverMessage()) {
        shuffleMessageDispatcher.getControlLinkListener().onSuccess(message);
      }
    }

    @Override
    public void onException(
        final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage shuffleMessage = message.getData().iterator().next();
      if (!shuffleMessage.isDriverMessage()) {
        shuffleMessageDispatcher.getControlLinkListener().onException(cause, remoteAddress, message);
      }
    }
  }
}
