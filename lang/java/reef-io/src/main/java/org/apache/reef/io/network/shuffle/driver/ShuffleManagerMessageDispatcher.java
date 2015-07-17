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
package org.apache.reef.io.network.shuffle.driver;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
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
public class ShuffleManagerMessageDispatcher {

  private static final Logger LOG = Logger.getLogger(ShuffleManagerMessageDispatcher.class.getName());

  private final ShuffleManager shuffleManager;

  private final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler;
  private final LinkListener<Message<ShuffleControlMessage>> controlLinkListener;

  private final Map<String, BroadcastEventHandler<Message<ShuffleControlMessage>>> controlMessageHandlerMap;
  private final Map<String, BroadcastLinkListener<Message<ShuffleControlMessage>>> controlLinkListenerMap;

  public ShuffleManagerMessageDispatcher(final ShuffleManager shuffleManager) {
    this.shuffleManager = shuffleManager;

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();

    this.controlMessageHandlerMap = new ConcurrentHashMap<>();
    this.controlLinkListenerMap = new ConcurrentHashMap<>();
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
      if (controlMessage.isMessageToManager()) {
        shuffleManager.onNext(message);
        return;
      }

      final EventHandler<Message<ShuffleControlMessage>> messageHandler = controlMessageHandlerMap.get(controlMessage.getGroupingName());

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
      if (controlMessage.isMessageToManager()) {
        shuffleManager.onSuccess(message);
        return;
      }

      final LinkListener<Message<ShuffleControlMessage>> linkListener = controlLinkListenerMap.get(controlMessage.getGroupingName());

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
      if (controlMessage.isMessageToManager()) {
        shuffleManager.onException(cause, remoteAddress, message);
        return;
      }

      final LinkListener<Message<ShuffleControlMessage>> linkListener = controlLinkListenerMap.get(controlMessage.getGroupingName());

      if (linkListener != null) {
        linkListener.onException(cause, remoteAddress, message);
      } else {
        LOG.log(Level.FINE, "An exception occurred while sending message [ {0} ] to {1}, cause [ {2} ], remote address [ {3} ].",
            new Object[]{ message, controlMessage.getGroupingName(), cause, remoteAddress });
      }
    }
  }
}
