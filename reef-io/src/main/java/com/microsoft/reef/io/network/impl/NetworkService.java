/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.impl;

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.naming.Naming;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.ConnectionFactory;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.TransportFactory;
import com.microsoft.reef.io.network.naming.NameCache;
import com.microsoft.reef.io.network.naming.NameClient;
import com.microsoft.reef.io.network.naming.NameLookupClient;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.*;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.LinkListener;
import com.microsoft.wake.remote.transport.Transport;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service for Task
 */
public final class NetworkService<T> implements Stage, ConnectionFactory<T> {

  private static final Logger LOG = Logger.getLogger(NetworkService.class.getName());

  private static final int retryCount;
  private static final int retryTimeout;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      retryCount = injector.getNamedInstance(NameLookupClient.RetryCount.class);
      retryTimeout = injector.getNamedInstance(NameLookupClient.RetryTimeout.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      LOG.log(Level.SEVERE, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  private Identifier myId;
  private final IdentifierFactory factory;
  private final Codec<T> codec;
  private final Transport transport;
  private final NameClient nameClient;

  private final ConcurrentMap<Identifier, Connection<T>> idToConnMap = new ConcurrentHashMap<>();

  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  private final EStage<Identifier> nameServiceUnregisteringStage;

  public NetworkService(final IdentifierFactory factory,
                        final int nsPort,
                        final String nameServerAddr,
                        final int nameServerPort,
                        final Codec<T> codec,
                        final TransportFactory tpFactory,
                        final EventHandler<Message<T>> recvHandler,
                        final EventHandler<Exception> exHandler) {
    this(factory, nsPort, nameServerAddr, nameServerPort,
        retryCount, retryTimeout, codec, tpFactory, recvHandler, exHandler);
  }

  @Inject
  public NetworkService(
      final @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) IdentifierFactory factory,
      final @Parameter(NetworkServiceParameters.NetworkServicePort.class) int nsPort,
      final @Parameter(NameServerParameters.NameServerAddr.class) String nameServerAddr,
      final @Parameter(NameServerParameters.NameServerPort.class) int nameServerPort,
      final @Parameter(NameLookupClient.RetryCount.class) int retryCount,
      final @Parameter(NameLookupClient.RetryTimeout.class) int retryTimeout,
      final @Parameter(NetworkServiceParameters.NetworkServiceCodec.class) Codec<T> codec,
      final @Parameter(NetworkServiceParameters.NetworkServiceTransportFactory.class) TransportFactory tpFactory,
      final @Parameter(NetworkServiceParameters.NetworkServiceHandler.class) EventHandler<Message<T>> recvHandler,
      final @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) EventHandler<Exception> exHandler) {

    this.factory = factory;
    this.codec = codec;
    this.transport = tpFactory.create(nsPort,
        new LoggingEventHandler<TransportEvent>(),
        new MessageHandler<T>(recvHandler, codec, factory), exHandler);

    this.nameClient = new NameClient(nameServerAddr, nameServerPort,
        factory, retryCount, retryTimeout, new NameCache(30000));

    this.nameServiceRegisteringStage = new SingleThreadStage<>(
        "NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {
      @Override
      public void onNext(final Tuple<Identifier, InetSocketAddress> tuple) {
        try {
          nameClient.register(tuple.getKey(), tuple.getValue());
          LOG.log(Level.FINEST, "Registered {0} with nameservice", tuple.getKey());
        } catch (final Exception ex) {
          final String msg = "Unable to register " + tuple.getKey() + "with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);

    this.nameServiceUnregisteringStage = new SingleThreadStage<>(
        "NameServiceRegisterer", new EventHandler<Identifier>() {
      @Override
      public void onNext(final Identifier id) {
        try {
          nameClient.unregister(id);
          LOG.log(Level.FINEST, "Unregistered {0} with nameservice", id);
        } catch (final Exception ex) {
          final String msg = "Unable to unregister " + id + " with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);
  }

  public void registerId(final Identifier id) {
    this.myId = id;
    final Tuple<Identifier, InetSocketAddress> tuple =
        new Tuple<>(id, (InetSocketAddress) this.transport.getLocalAddress());
    LOG.log(Level.FINEST, "Binding {0} to NetworkService@({1})",
        new Object[]{tuple.getKey(), tuple.getValue()});
    this.nameServiceRegisteringStage.onNext(tuple);
  }

  public void unregisterId(Identifier id) {
    this.myId = null;
    LOG.log(Level.FINEST, "Unbinding {0} to NetworkService@({1})",
        new Object[]{id, this.transport.getLocalAddress()});
    this.nameServiceUnregisteringStage.onNext(id);
  }

  public Identifier getMyId() {
    return this.myId;
  }

  public Transport getTransport() {
    return this.transport;
  }

  public Codec<T> getCodec() {
    return this.codec;
  }

  public Naming getNameClient() {
    return this.nameClient;
  }

  public IdentifierFactory getIdentifierFactory() {
    return this.factory;
  }

  void remove(final Identifier id) {
    this.idToConnMap.remove(id);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "Shutting down");
    this.transport.close();
    this.nameClient.close();
  }

  @Override
  public Connection<T> newConnection(final Identifier destId) {

    if (this.myId == null) {
      throw new RuntimeException(
          "Trying to establish a connection from a Network Service that is not bound to any task");
    }

    final Connection<T> conn = this.idToConnMap.get(destId);
    if (conn != null) {
      return conn;
    }

    final Connection<T> newConnection = new NSConnection<T>(
        this.myId, destId, new LinkListener<T>() {
      @Override
      public void messageReceived(final Object message) {
      }
    }, this);

    final Connection<T> existing = this.idToConnMap.putIfAbsent(destId, newConnection);
    return existing == null ? newConnection : existing;
  }
}

class MessageHandler<T> implements EventHandler<TransportEvent> {

  private final EventHandler<Message<T>> handler;
  private final NSMessageCodec<T> codec;

  public MessageHandler(final EventHandler<Message<T>> handler,
                        final Codec<T> codec, final IdentifierFactory factory) {
    this.handler = handler;
    this.codec = new NSMessageCodec<T>(codec, factory);
  }

  @Override
  public void onNext(final TransportEvent value) {
    final byte[] data = value.getData();
    final NSMessage<T> obj = this.codec.decode(data);
    this.handler.onNext(obj);
  }
}
