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
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.shuffle.utils.BroadcastEventHandler;
import org.apache.reef.io.network.shuffle.utils.BroadcastLinkListener;
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
public final class ShuffleClientMessageDispatcher {

  private static final Logger LOG = Logger.getLogger(ShuffleClientMessageDispatcher.class.getName());

  private final ShuffleClient client;

  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;
  private final EventHandler<Message<ShuffleTupleMessage>> tupleMessageHandler;
  private final LinkListener<Message<ShuffleTupleMessage>> tupleLinkListener;

  private final Map<String, BroadcastEventHandler<Message<ShuffleControlMessage>>> senderControlMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleControlMessage>>> senderControlLinkListenerMap;
  private final Map<String, BroadcastEventHandler<Message<ShuffleControlMessage>>> receiverControlMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleControlMessage>>> receiverControlLinkListenerMap;
  private final Map<String, BroadcastEventHandler<Message<ShuffleTupleMessage>>> tupleMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleTupleMessage>>> tupleLinkListenerMap;

  public ShuffleClientMessageDispatcher(final ShuffleClient client) {
    this.client = client;

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
    this.tupleMessageHandler = new TupleMessageHandler();
    this.tupleLinkListener = new TupleLinkListener();

    this.senderControlMessageHandlerMap = new ConcurrentHashMap<>();
    this.senderControlLinkListenerMap = new ConcurrentHashMap<>();
    this.receiverControlMessageHandlerMap = new ConcurrentHashMap<>();
    this.receiverControlLinkListenerMap = new ConcurrentHashMap<>();
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

  public void registerSenderControlLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    if (!senderControlLinkListenerMap.containsKey(groupingName)) {
      senderControlLinkListenerMap.put(groupingName, new BroadcastLinkListener());
    }

    senderControlLinkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  public void registerSenderControlMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleControlMessage>> messageHandler) {
    if (!senderControlMessageHandlerMap.containsKey(groupingName)) {
      senderControlMessageHandlerMap.put(groupingName, new BroadcastEventHandler());
    }
    senderControlMessageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  public void registerReceiverControlLinkListener(
      final String groupingName, final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    if (!receiverControlLinkListenerMap.containsKey(groupingName)) {
      receiverControlLinkListenerMap.put(groupingName, new BroadcastLinkListener());
    }

    receiverControlLinkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  public void registerReceiverControlMessageHandler(
      final String groupingName, final EventHandler<Message<ShuffleControlMessage>> messageHandler) {
    if (!receiverControlMessageHandlerMap.containsKey(groupingName)) {
      receiverControlMessageHandlerMap.put(groupingName, new BroadcastEventHandler());
    }
    receiverControlMessageHandlerMap.get(groupingName).addEventHandler(messageHandler);
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
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      if (controlMessage.isMessageToClient()) {
        client.onNext(message);
        return;
      }

      final EventHandler<Message<ShuffleControlMessage>> messageHandler;
      if (controlMessage.isMessageToSender()) {
        messageHandler = senderControlMessageHandlerMap.get(controlMessage.getGroupingName());
      } else if (controlMessage.isMessageToReceiver()) {
        messageHandler = receiverControlMessageHandlerMap.get(controlMessage.getGroupingName());
      } else {
        messageHandler = null;
      }

      if (messageHandler != null) {
        messageHandler.onNext(message);
      } else {
        LOG.log(Level.FINE, "There is no message handler registered for {0}. The arrived message is {1}",
            new Object[]{ controlMessage.getGroupingName(), message});
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      if (controlMessage.isMessageToClient()) {
        client.onSuccess(message);
        return;
      }

      final LinkListener<Message<ShuffleControlMessage>> linkListener;
      if (controlMessage.isMessageToSender()) {
        linkListener = senderControlLinkListenerMap.get(controlMessage.getGroupingName());
      } else if (controlMessage.isMessageToReceiver()) {
        linkListener = receiverControlLinkListenerMap.get(controlMessage.getGroupingName());
      } else {
        linkListener = null;
      }

      if (linkListener != null) {
        linkListener.onSuccess(message);
      } else {
        LOG.log(Level.FINE, "The message [ {0} ] was successfully sent to {1}",
            new Object[]{ message, controlMessage.getGroupingName() });
      }
    }

    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      if (controlMessage.isMessageToClient()) {
        client.onException(cause, remoteAddress, message);
        return;
      }

      final LinkListener<Message<ShuffleControlMessage>> linkListener;
      if (controlMessage.isMessageToSender()) {
        linkListener = senderControlLinkListenerMap.get(controlMessage.getGroupingName());
      } else if (controlMessage.isMessageToReceiver()) {
        linkListener = receiverControlLinkListenerMap.get(controlMessage.getGroupingName());
      } else {
        linkListener = null;
      }

      if (linkListener != null) {
        linkListener.onException(cause, remoteAddress, message);
      } else {
        LOG.log(Level.FINE, "An exception occurred while sending message [ {0} ] to {1}, cause [ {2} ], remote address [ {3} ].",
            new Object[]{ message, controlMessage.getGroupingName(), cause, remoteAddress });
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
