/*
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
package org.apache.reef.io.network.impl;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.util.Map;

/**
 * NetworkConnectionService event handler.
 * It dispatches events to the corresponding eventHandler.
 */
final class NetworkConnectionServiceReceiveHandler implements EventHandler<TransportEvent> {

  private final Map<String, NetworkConnectionFactory> connFactoryMap;
  private final Codec<NetworkConnectionServiceMessage> codec;

  NetworkConnectionServiceReceiveHandler(
      final Map<String, NetworkConnectionFactory> connFactoryMap,
      final Codec<NetworkConnectionServiceMessage> codec) {
    this.connFactoryMap = connFactoryMap;
    this.codec = codec;
  }

  @Override
  public void onNext(final TransportEvent transportEvent) {
    final NetworkConnectionServiceMessage nsMessage = codec.decode(transportEvent.getData());
    nsMessage.setRemoteAddress(transportEvent.getRemoteAddress());
    final NetworkConnectionFactory connFactory = connFactoryMap.get(nsMessage.getConnectionFactoryId());
    final EventHandler eventHandler = connFactory.getEventHandler();
    eventHandler.onNext(nsMessage);
  }
}
