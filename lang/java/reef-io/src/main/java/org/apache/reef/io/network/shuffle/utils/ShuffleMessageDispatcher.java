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
package org.apache.reef.io.network.shuffle.utils;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class ShuffleMessageDispatcher {

  private static final Logger LOG = Logger.getLogger(ShuffleMessageDispatcher.class.getName());

  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;
  private final EventHandler<Message<ShuffleTupleMessage>> tupleMessageHandler;
  private final LinkListener<Message<ShuffleTupleMessage>> tupleLinkListener;

  private final Map<String, BroadcastEventHandler<Message<ShuffleControlMessage>>> controlMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleControlMessage>>> controlLinkListenerMap;
  private final Map<String, BroadcastEventHandler<Message<ShuffleTupleMessage>>> tupleMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleTupleMessage>>> tupleLinkListenerMap;

  public ShuffleMessageDispatcher() {
    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
    this.tupleMessageHandler = new TupleMessageHandler();
    this.tupleLinkListener = new TupleLinkListener();

    this.controlMessageHandlerMap = new ConcurrentHashMap<>();
    this.controlLinkListenerMap = new ConcurrentHashMap<>();
    this.tupleMessageHandlerMap = new ConcurrentHashMap<>();
    this.tupleLinkListenerMap = new ConcurrentHashMap<>();
  }

  public <K, V> void registerTupleLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    if (!tupleLinkListenerMap.containsKey(groupingName)) {
      tupleLinkListenerMap.put(groupingName, new BroadcastLinkListener());
    }

    tupleLinkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  public <K, V> void registerTupleMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    if (!tupleMessageHandlerMap.containsKey(groupingName)) {
      tupleMessageHandlerMap.put(groupingName, new BroadcastEventHandler());
    }
    tupleMessageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  public void registerControlLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    if (!controlLinkListenerMap.containsKey(groupingName)) {
      controlLinkListenerMap.put(groupingName, new BroadcastLinkListener());
    }

    controlLinkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  public void registerControlMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleControlMessage>> messageHandler) {
    if (!controlMessageHandlerMap.containsKey(groupingName)) {
      controlMessageHandlerMap.put(groupingName, new BroadcastEventHandler());
    }
    controlMessageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  public EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler() {
    return tupleMessageHandler;
  }

  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return tupleLinkListener;
  }

  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return controlMessageHandler;
  }

  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return controlLinkListener;
  }


  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final EventHandler<Message<ShuffleControlMessage>> messageHandler = controlMessageHandlerMap.get(groupingName);

      if (messageHandler != null) {
        messageHandler.onNext(message);
      } else {
        LOG.log(Level.FINE, "There is no message handler registered for {0}. The arrived message is {1}",
            new Object[]{ groupingName, message});
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final LinkListener<Message<ShuffleControlMessage>> linkListener = controlLinkListenerMap.get(groupingName);

      if (linkListener != null) {
        linkListener.onSuccess(message);
      } else {
        LOG.log(Level.FINE, "The message [ {0} ] was successfully sent to {1}",
            new Object[]{ message, groupingName });
      }
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final LinkListener<Message<ShuffleControlMessage>> linkListener = controlLinkListenerMap.get(groupingName);

      if (linkListener != null) {
        linkListener.onException(cause, remoteAddress, message);
      } else {
        LOG.log(Level.FINE, "An exception occurred while sending message [ {0} ] to {1}, cause [ {2} ], remote address [ {3} ].",
            new Object[]{ message, groupingName, cause, remoteAddress });
      }
    }
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final EventHandler<Message<ShuffleTupleMessage>> messageHandler = tupleMessageHandlerMap.get(groupingName);

      if (messageHandler != null) {
        messageHandler.onNext(message);
      } else {
        LOG.log(Level.FINE, "There is no message handler registered for {0}. The arrived message is {1}",
            new Object[]{ groupingName, message});
      }
    }
  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final LinkListener<Message<ShuffleTupleMessage>> linkListener = tupleLinkListenerMap.get(groupingName);

      if (linkListener != null) {
        linkListener.onSuccess(message);
      } else {
        LOG.log(Level.FINE, "The message [ {0} ] was successfully sent to {1}",
            new Object[]{ message, groupingName });
      }
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final LinkListener<Message<ShuffleTupleMessage>> linkListener = tupleLinkListenerMap.get(groupingName);

      if (linkListener != null) {
        linkListener.onException(cause, remoteAddress, message);
      } else {
        LOG.log(Level.FINE, "An exception occurred while sending message [ {0} ] to {1}, cause [ {2} ], remote address [ {3} ].",
            new Object[]{ message, groupingName, cause, remoteAddress });
      }
    }
  }
}
