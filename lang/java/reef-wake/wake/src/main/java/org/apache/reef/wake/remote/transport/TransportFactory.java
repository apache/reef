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
package org.apache.reef.wake.remote.transport;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;

/**
 * Factory that creates a transport.
 */
@DefaultImplementation(MessagingTransportFactory.class)
public interface TransportFactory {

  /**
   * Types of protocol used in Transport.
   */
  enum ProtocolType {
    TCP, HTTP
  }

  /**
   * Creates a transport.
   *
   * @param port          a listening port
   * @param clientHandler a transport client-side handler
   * @param serverHandler a transport server-side handler
   * @param exHandler     an exception handler
   * @return transport
   */
  Transport newInstance(int port,
                        EventHandler<TransportEvent> clientHandler,
                        EventHandler<TransportEvent> serverHandler,
                        EventHandler<Exception> exHandler);

  /**
   * Creates a transport.
   *
   * @param hostAddress     a host address
   * @param port            a listening port
   * @param clientStage     a transport client-side stage
   * @param serverStage     a transport server-side stage
   * @param numberOfTries   the number of retries for connection
   * @param retryTimeout    retry timeout
   * @return transport
   */
  Transport newInstance(String hostAddress, int port,
                        EStage<TransportEvent> clientStage,
                        EStage<TransportEvent> serverStage,
                        int numberOfTries,
                        int retryTimeout);

  /**
   * Creates a transport.
   *
   * @param hostAddress     a host address
   * @param port            a listening port
   * @param clientStage     a transport client-side stage
   * @param serverStage     a transport server-side stage
   * @param numberOfTries   the number of retries for connection
   * @param retryTimeout    retry timeout
   * @param tcpPortProvider tcpPortProvider
   * @return transport
   */
  Transport newInstance(String hostAddress,
                        int port,
                        EStage<TransportEvent> clientStage,
                        EStage<TransportEvent> serverStage,
                        int numberOfTries,
                        int retryTimeout,
                        TcpPortProvider tcpPortProvider);


}
