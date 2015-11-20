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
package org.apache.reef.wake.remote;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;

/**
 * Default implementation of RemoteManagerFactory.
 */
final class DefaultRemoteManagerFactory implements RemoteManagerFactory {

  private final Injector injector;

  private final Codec<?> codec;
  private final EventHandler<Throwable> errorHandler;
  private final boolean orderingGuarantee;
  private final int numberOfTries;
  private final int retryTimeout;
  private final LocalAddressProvider localAddressProvider;
  private final TransportFactory transportFactory;
  private final TcpPortProvider tcpPortProvider;

  @Inject
  private DefaultRemoteManagerFactory(
      @Parameter(RemoteConfiguration.MessageCodec.class) final Codec<?> codec,
      @Parameter(RemoteConfiguration.ErrorHandler.class) final EventHandler<Throwable> errorHandler,
      @Parameter(RemoteConfiguration.OrderingGuarantee.class) final boolean orderingGuarantee,
      @Parameter(RemoteConfiguration.NumberOfTries.class) final int numberOfTries,
      @Parameter(RemoteConfiguration.RetryTimeout.class) final int retryTimeout,
      final LocalAddressProvider localAddressProvider,
      final TransportFactory tpFactory,
      final TcpPortProvider tcpPortProvider,
      final Injector injector) {
    this.injector = injector.forkInjector();
    this.codec = codec;
    this.errorHandler = errorHandler;
    this.orderingGuarantee = orderingGuarantee;
    this.numberOfTries = numberOfTries;
    this.retryTimeout = retryTimeout;
    this.localAddressProvider = localAddressProvider;
    this.transportFactory = tpFactory;
    this.tcpPortProvider = tcpPortProvider;
  }

  @Override
  public RemoteManager getInstance(final String name) {
    try {
      final Injector newInjector = injector.forkInjector();
      newInjector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, name);
      newInjector.bindVolatileParameter(RemoteConfiguration.MessageCodec.class, this.codec);
      newInjector.bindVolatileParameter(RemoteConfiguration.ErrorHandler.class, this.errorHandler);
      newInjector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, this.orderingGuarantee);
      newInjector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, this.numberOfTries);
      newInjector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, this.retryTimeout);
      newInjector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      newInjector.bindVolatileInstance(TransportFactory.class, this.transportFactory);
      newInjector.bindVolatileInstance(TcpPortProvider.class, this.tcpPortProvider);
      return newInjector.getInstance(RemoteManager.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public <T> RemoteManager getInstance(final String name,
                                       final String hostAddress,
                                       final int listeningPort,
                                       final Codec<T> codec,
                                       final EventHandler<Throwable> errorHandler,
                                       final boolean orderingGuarantee,
                                       final int numberOfTries,
                                       final int retryTimeout,
                                       final LocalAddressProvider localAddressProvider,
                                       final TcpPortProvider tcpPortProvider) {
    try {
      final Injector newInjector = injector.forkInjector();
      newInjector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, name);
      newInjector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, hostAddress);
      newInjector.bindVolatileParameter(RemoteConfiguration.Port.class, listeningPort);
      newInjector.bindVolatileParameter(RemoteConfiguration.MessageCodec.class, codec);
      newInjector.bindVolatileParameter(RemoteConfiguration.ErrorHandler.class, errorHandler);
      newInjector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, orderingGuarantee);
      newInjector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, numberOfTries);
      newInjector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, retryTimeout);
      newInjector.bindVolatileInstance(LocalAddressProvider.class, localAddressProvider);
      newInjector.bindVolatileInstance(TransportFactory.class, this.transportFactory);
      newInjector.bindVolatileInstance(TcpPortProvider.class, tcpPortProvider);
      return newInjector.getInstance(RemoteManager.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public <T> RemoteManager getInstance(final String name,
                                       final String hostAddress,
                                       final int listeningPort,
                                       final Codec<T> codec,
                                       final EventHandler<Throwable> errorHandler,
                                       final boolean orderingGuarantee,
                                       final int numberOfTries,
                                       final int retryTimeout) {
    try {
      final Injector newInjector = injector.forkInjector();
      newInjector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, name);
      newInjector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, hostAddress);
      newInjector.bindVolatileParameter(RemoteConfiguration.Port.class, listeningPort);
      newInjector.bindVolatileParameter(RemoteConfiguration.MessageCodec.class, codec);
      newInjector.bindVolatileParameter(RemoteConfiguration.ErrorHandler.class, errorHandler);
      newInjector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, orderingGuarantee);
      newInjector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, numberOfTries);
      newInjector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, retryTimeout);
      newInjector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      newInjector.bindVolatileInstance(TransportFactory.class, this.transportFactory);
      newInjector.bindVolatileInstance(TcpPortProvider.class, this.tcpPortProvider);
      return newInjector.getInstance(RemoteManager.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public <T> RemoteManager getInstance(
      final String name, final Codec<T> codec, final EventHandler<Throwable> errorHandler) {
    try {
      final Injector newInjector = injector.forkInjector();
      newInjector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, name);
      newInjector.bindVolatileParameter(RemoteConfiguration.MessageCodec.class, codec);
      newInjector.bindVolatileParameter(RemoteConfiguration.ErrorHandler.class, errorHandler);
      newInjector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, this.orderingGuarantee);
      newInjector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, this.numberOfTries);
      newInjector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, this.retryTimeout);
      newInjector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      newInjector.bindVolatileInstance(TransportFactory.class, this.transportFactory);
      newInjector.bindVolatileInstance(TcpPortProvider.class, this.tcpPortProvider);
      return newInjector.getInstance(RemoteManager.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:hiddenfield")
  public <T> RemoteManager getInstance(final String name,
                                       final int listeningPort,
                                       final Codec<T> codec,
                                       final EventHandler<Throwable> errorHandler) {
    try {
      final Injector newInjector = injector.forkInjector();
      newInjector.bindVolatileParameter(RemoteConfiguration.ManagerName.class, name);
      newInjector.bindVolatileParameter(RemoteConfiguration.Port.class, listeningPort);
      newInjector.bindVolatileParameter(RemoteConfiguration.MessageCodec.class, codec);
      newInjector.bindVolatileParameter(RemoteConfiguration.ErrorHandler.class, errorHandler);
      newInjector.bindVolatileParameter(RemoteConfiguration.OrderingGuarantee.class, this.orderingGuarantee);
      newInjector.bindVolatileParameter(RemoteConfiguration.NumberOfTries.class, this.numberOfTries);
      newInjector.bindVolatileParameter(RemoteConfiguration.RetryTimeout.class, this.retryTimeout);
      newInjector.bindVolatileInstance(LocalAddressProvider.class, this.localAddressProvider);
      newInjector.bindVolatileInstance(TransportFactory.class, this.transportFactory);
      newInjector.bindVolatileInstance(TcpPortProvider.class, this.tcpPortProvider);
      return newInjector.getInstance(RemoteManager.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
