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

/**
 *
 */
public final class StaticShuffleClient implements ShuffleClient {

  private boolean isTopologySetup;

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
  }

  @Override
  public ShuffleDescriptor getShuffleDescriptor() {
    return initialShuffleDescriptor;
  }

  @Override
  public boolean waitForSetup() {
    if (isTopologySetup) {
      return false;
    } else {
      synchronized (this) {
        try {
          while (!isTopologySetup) {
            this.wait();
          }

          return true;
        } catch (final InterruptedException e) {
          throw new RuntimeException("An InterruptedException occurred while waiting for topology set up", e);
        }
      }
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
  public <K, V> TupleReceiver<K, V> getReceiver(final String groupingName) {
    return tupleOperatorFactory.newTupleReceiver(initialShuffleDescriptor.getGroupingDescriptor(groupingName));
  }

  @Override
  public <K, V> TupleSender<K, V> getSender(final String groupingName) {
    return tupleOperatorFactory.newTupleSender(initialShuffleDescriptor.getGroupingDescriptor(groupingName));
  }

  @Override
  public <K, V> void registerLinkListener(final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    tupleMessageDispatcher.registerLinkListener(groupingName, linkListener);
  }

  @Override
  public <K, V> void registerMessageHandler(final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
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
      if (shuffleMessage.getCode() == StaticShuffleMessageCode.SHUFFLE_SETUP) {
        synchronized (StaticShuffleClient.this) {
          isTopologySetup = true;
          StaticShuffleClient.this.notifyAll();
        }
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
