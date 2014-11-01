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

import org.apache.reef.io.network.TransportFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;

/**
 * Factory that creates a messaging transport
 */
public class MessagingTransportFactory implements TransportFactory {

  @Inject
  public MessagingTransportFactory() {
  }

  /**
   * Creates a transport
   *
   * @param port          a listening port
   * @param clientHandler a transport client side handler
   * @param serverHandler a transport server side handler
   * @param exHandler     a exception handler
   */
  @Override
  public Transport create(final int port,
                          final EventHandler<TransportEvent> clientHandler,
                          final EventHandler<TransportEvent> serverHandler,
                          final EventHandler<Exception> exHandler) {

    final Transport transport = new NettyMessagingTransport(NetUtils.getLocalAddress(),
        port, new SyncStage<>(clientHandler), new SyncStage<>(serverHandler), 3, 10000);

    transport.registerErrorHandler(exHandler);
    return transport;
  }
}
