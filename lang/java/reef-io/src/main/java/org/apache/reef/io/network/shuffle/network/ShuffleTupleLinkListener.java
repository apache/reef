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
package org.apache.reef.io.network.shuffle.network;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class ShuffleTupleLinkListener implements LinkListener<Message<ShuffleTupleMessage>> {

  private static final Logger LOG = Logger.getLogger(ShuffleTupleLinkListener.class.getName());

  private final Map<String, Map<String, LinkListener>> linkListenerMap;

  @Inject
  public ShuffleTupleLinkListener() {
    this.linkListenerMap = new ConcurrentHashMap<>();
  }

  public <K, V> void registerLinkListener(final String shuffleGroupName, final String shuffleName,
                            final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    if (!linkListenerMap.containsKey(shuffleGroupName)) {
      linkListenerMap.put(shuffleGroupName, new ConcurrentHashMap<String, LinkListener>());
    }

    linkListenerMap.get(shuffleGroupName).put(shuffleName, linkListener);
  }

  @Override
  public void onSuccess(final Message<ShuffleTupleMessage> message) {
    final ShuffleTupleMessage tupleMessage = message.getData().iterator().next();
    final LinkListener<Message<ShuffleTupleMessage>> linkListener = linkListenerMap
        .get(tupleMessage.getShuffleGroupName()).get(tupleMessage.getShuffleName());
    if (linkListener != null) {
      linkListener.onSuccess(message);
    } else {
      LOG.log(Level.INFO, "There is no registered link listener for {0}:{1}. An message was successfully sent {2}.",
          new Object[]{tupleMessage.getShuffleGroupName(), tupleMessage.getShuffleName(), message});
    }
  }

  @Override
  public void onException(
      final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleTupleMessage> message) {
    final ShuffleTupleMessage tupleMessage = message.getData().iterator().next();
    final LinkListener<Message<ShuffleTupleMessage>> linkListener = linkListenerMap
        .get(tupleMessage.getShuffleGroupName()).get(tupleMessage.getShuffleName());
    if (linkListener != null) {
      linkListener.onException(cause, remoteAddress, message);
    } else {
      LOG.log(Level.INFO, "There is no registered link listener for {0}:{1}. An exception occurred while sending {2}.",
          new Object[]{tupleMessage.getShuffleGroupName(), tupleMessage.getShuffleName(), message});
    }
  }
}
