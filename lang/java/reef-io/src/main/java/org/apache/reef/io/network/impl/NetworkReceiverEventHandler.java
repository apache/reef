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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.NetworkEvent;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives a network event from Transport, and wraps it with NetworkServiceEvent
 */
final class NetworkReceiverEventHandler implements EventHandler<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(NetworkReceiverEventHandler.class.getName());

  private final IdentifierFactory idFactory;
  private final Codec<NetworkServiceEvent> codec;
  private final NetworkPreconfiguredMap preconfiguredMap;

  NetworkReceiverEventHandler(
      final IdentifierFactory idFactory,
      final NetworkPreconfiguredMap preconfiguredMap,
      final Codec<NetworkServiceEvent> codec) {
    this.idFactory = idFactory;
    this.codec = codec;
    this.preconfiguredMap = preconfiguredMap;
  }

  /**
   * Handles the event received from a remote node
   *
   * @param e the event
   */
  @Override
  public void onNext(TransportEvent e) {
    final NetworkServiceEvent networkServiceEvent = codec.decode(e.getData());
    if (LOG.isLoggable(Level.FINER)) {
      LOG.log(Level.FINER, "{0} {1}", new Object[]{e, networkServiceEvent});
    }

    final Identifier remoteId = networkServiceEvent.getRemoteId() != null ?
        idFactory.getNewInstance(networkServiceEvent.getRemoteId()) : null;

    NetworkEvent<?> parsedEvent = new NetworkEvent<>(
        networkServiceEvent.remoteAddress(),
        remoteId,
        networkServiceEvent.getDataList()
    );

    preconfiguredMap.getEventHandler(networkServiceEvent.getEventClassNameCode()).onNext(parsedEvent);
  }
}
