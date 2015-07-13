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
package org.apache.reef.io.network.shuffle.ns;

import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
final class ShuffleControlLinkListenerImpl implements ShuffleControlLinkListener {

  private final Map<String, LinkListener<Message<ShuffleControlMessage>>> linkListenerMap;

  @Inject
  public ShuffleControlLinkListenerImpl() {
    linkListenerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void onSuccess(final Message<ShuffleControlMessage> message) {
    linkListenerMap.get(getShuffleNameFrom(message)).onSuccess(message);
  }

  @Override
  public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<ShuffleControlMessage> message) {
    linkListenerMap.get(getShuffleNameFrom(message)).onSuccess(message);
  }

  private String getShuffleNameFrom(final Message<ShuffleControlMessage> message) {
    return message.getData().iterator().next().getShuffleName();
  }

  @Override
  public void registerLinkListener(final Class<? extends Name<String>> shuffleName,
                                   final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    linkListenerMap.put(shuffleName.getName(), linkListener);
  }
}
