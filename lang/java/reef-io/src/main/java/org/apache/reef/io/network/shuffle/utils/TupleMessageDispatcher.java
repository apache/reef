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
import org.apache.reef.io.network.shuffle.ns.ShuffleTupleMessage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class TupleMessageDispatcher {

  private static final Logger LOG = Logger.getLogger(TupleMessageDispatcher.class.getName());

  private final EventHandler<Message<ShuffleTupleMessage>> tupleMessageHandler;
  private final LinkListener<Message<ShuffleTupleMessage>> tupleLinkListener;

  private final Map<String, BroadcastEventHandler<Message<ShuffleTupleMessage>>> messageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleTupleMessage>>> linkListenerMap;

  public TupleMessageDispatcher() {
    this.tupleMessageHandler = new TupleMessageHandler();
    this.tupleLinkListener = new TupleLinkListener();
    this.messageHandlerMap = new HashMap<>();
    this.linkListenerMap = new HashMap<>();
  }

  public <K, V> void registerLinkListener(final String groupingName, final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    if (!linkListenerMap.containsKey(groupingName)) {
      linkListenerMap.put(groupingName, new BroadcastLinkListener());
    }

    linkListenerMap.get(groupingName).addLinkListener(linkListener);
  }

  public <K, V> void registerMessageHandler(final String groupingName, final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    if (!messageHandlerMap.containsKey(groupingName)) {
      messageHandlerMap.put(groupingName, new BroadcastEventHandler());
    }
    messageHandlerMap.get(groupingName).addEventHandler(messageHandler);
  }

  public EventHandler<Message<ShuffleTupleMessage>> getTupleMessageHandler() {
    return tupleMessageHandler;
  }

  public LinkListener<Message<ShuffleTupleMessage>> getTupleLinkListener() {
    return tupleLinkListener;
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage> message) {
      final String groupingName = message.getData().iterator().next().getGroupingName();
      final EventHandler<Message<ShuffleTupleMessage>> messageHandler = messageHandlerMap.get(groupingName);

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
      final LinkListener<Message<ShuffleTupleMessage>> linkListener = linkListenerMap.get(groupingName);

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
      final LinkListener<Message<ShuffleTupleMessage>> linkListener = linkListenerMap.get(groupingName);

      if (linkListener != null) {
        linkListener.onException(cause, remoteAddress, message);
      } else {
        LOG.log(Level.FINE, "An exception occurred while sending message [ {0} ] to {1}, cause [ {2} ], remote address [ {3} ].",
            new Object[]{ message, groupingName, cause, remoteAddress });
      }
    }
  }
}
