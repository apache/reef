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
package org.apache.reef.wake.remote.transport.netty.http;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;

/**
 * Factory that creates a messaging transport.
 */
public final class HttpMessagingTransportFactory implements TransportFactory {

  private final String localAddress;

  @Inject
  private HttpMessagingTransportFactory(final LocalAddressProvider localAddressProvider) {
    this.localAddress = localAddressProvider.getLocalAddress();
  }

  /**
   * Creates a transport.
   *
   * @param port          a listening port
   * @param clientHandler a transport client side handler
   * @param serverHandler a transport server side handler
   * @param exHandler     a exception handler
   */
  @Override
  public Transport newInstance(final int port,
                               final EventHandler<TransportEvent> clientHandler,
                               final EventHandler<TransportEvent> serverHandler,
                               final EventHandler<Exception> exHandler) {

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, this.localAddress);
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, port);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteClientStage.class, new SyncStage<>(clientHandler));
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class, new SyncStage<>(serverHandler));

    final Transport transport;
    try {
      transport = injector.getInstance(NettyHttpMessagingTransport.class);
      transport.registerErrorHandler(exHandler);
      return transport;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a transport.
   *
   * @param hostAddress   a host address
   * @param port          a listening port
   * @param clientStage   a client stage
   * @param serverStage   a server stage
   * @param numberOfTries a number of tries
   * @param retryTimeout  a timeout for retry
   */
  @Override
  public Transport newInstance(final String hostAddress,
                               final int port,
                               final EStage<TransportEvent> clientStage,
                               final EStage<TransportEvent> serverStage,
                               final int numberOfTries,
                               final int retryTimeout) {
    try {
      TcpPortProvider tcpPortProvider = Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);
      return newInstance(hostAddress, port, clientStage,
              serverStage, numberOfTries, retryTimeout, tcpPortProvider);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a transport.
   *
   * @param hostAddress     a host address
   * @param port            a listening port
   * @param clientStage     a client stage
   * @param serverStage     a server stage
   * @param numberOfTries   a number of tries
   * @param retryTimeout    a timeout for retry
   * @param tcpPortProvider a provider for TCP port
   */
  @Override
  public Transport newInstance(final String hostAddress,
                               final int port,
                               final EStage<TransportEvent> clientStage,
                               final EStage<TransportEvent> serverStage,
                               final int numberOfTries,
                               final int retryTimeout,
                               final TcpPortProvider tcpPortProvider) {

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, hostAddress);
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, port);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteClientStage.class, clientStage);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class, serverStage);
    injector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, numberOfTries);
    injector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, retryTimeout);
    injector.bindVolatileInstance(TcpPortProvider.class, tcpPortProvider);
    try {
      return injector.getInstance(NettyHttpMessagingTransport.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
