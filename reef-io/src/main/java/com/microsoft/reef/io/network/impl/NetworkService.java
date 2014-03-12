/**
 * Copyright (C) 2013 Microsoft Corporation
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
public class NetworkService<T> implements Stage, ConnectionFactory<T> {

  private static final Logger LOG = Logger.getLogger(NetworkService.class.getName());

  private static final int retryCount, retryTimeout;

  static {
    Tang tang = Tang.Factory.getTang();
    try {
      retryCount = tang.newInjector().getNamedInstance(NameLookupClient.RetryCount.class);
      retryTimeout = tang.newInjector().getNamedInstance(NameLookupClient.RetryTimeout.class);
    } catch (InjectionException e1) {
      throw new RuntimeException("Exception while trying to find default values for retryCount & Timeout", e1);
    }
  }

  private Identifier myId;
  private final IdentifierFactory factory;
  private final Codec<T> codec;
  private final Transport transport;
  private final NameClient nameClient;

  private final ConcurrentMap<Identifier, Connection<T>> idToConnMap = new ConcurrentHashMap<Identifier, Connection<T>>();

  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  private final EStage<Identifier> nameServiceUnregisteringStage;

  public NetworkService(
      IdentifierFactory factory,
      int nsPort,
      String nameServerAddr,
      int nameServerPort,
      Codec<T> codec,
      TransportFactory tpFactory,
      EventHandler<Message<T>> recvHandler,
      EventHandler<Exception> exHandler
  ) {
    this(factory, nsPort, nameServerAddr, nameServerPort, retryCount, retryTimeout, codec, tpFactory, recvHandler, exHandler);
  }

  @Inject
  public NetworkService(
      final @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) IdentifierFactory factory,
      @Parameter(NetworkServiceParameters.NetworkServicePort.class) int nsPort,
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
    this.transport = tpFactory.create(nsPort, new LoggingEventHandler<TransportEvent>(),
        new MessageHandler<T>(recvHandler, codec, factory), exHandler);
    this.nameClient = new NameClient(nameServerAddr, nameServerPort, factory, retryCount, retryTimeout, new NameCache(30000));
    nsPort = transport.getListeningPort();

    nameServiceRegisteringStage = new SingleThreadStage<>("NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {
      @Override
      public void onNext(Tuple<Identifier, InetSocketAddress> tuple) {
        try {
          nameClient.register(tuple.getKey(), tuple.getValue());
          LOG.fine("Finished registering " + tuple.getKey() + " with nameservice");
          LOG.log(Level.FINEST, "Finished registering " + tuple.getKey() + " with nameservice");
        } catch (Exception e) {
          throw new RuntimeException("Unable to register " + tuple.getKey() + "with name service", e);
        }
      }
    }, 5);

    nameServiceUnregisteringStage = new SingleThreadStage<>("NameServiceRegisterer", new EventHandler<Identifier>() {
      @Override
      public void onNext(Identifier id) {
        try {
          nameClient.unregister(id);
          LOG.fine("Finished unregistering " + id + " from nameservice");
          LOG.log(Level.FINEST, "Finished unregistering " + id + " from nameservice");
        } catch (Exception e) {
          throw new RuntimeException("Unable to unregister " + id + " with name service", e);
        }
      }
    }, 5);
  }

  public void registerId(Identifier id) {
    this.myId = id;
    final Tuple<Identifier, InetSocketAddress> tuple = new Tuple<>(id, (InetSocketAddress) transport.getLocalAddress());
    LOG.log(Level.FINEST, "Binding " + tuple.getKey() + " to NetworkService@(" + tuple.getValue() + ")");
    nameServiceRegisteringStage.onNext(tuple);
  }

  public void unregisterId(Identifier id) {
    this.myId = null;
    LOG.log(Level.FINEST, "Unbinding " + id + " from NetworkService@(" + transport.getLocalAddress() + ")");
    nameServiceUnregisteringStage.onNext(id);
  }

  public Identifier getMyId() {
    return myId;
  }

  public Transport getTransport() {
    return transport;
  }

  public Codec<T> getCodec() {
    return codec;
  }

  public Naming getNameClient() {
    return nameClient;
  }

  public IdentifierFactory getIdentifierFactory() {
    return factory;
  }

  void remove(final Identifier id) {
    idToConnMap.remove(id);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.INFO, "Shutting down");
    transport.close();
    nameClient.close();
  }

  @Override
  public Connection<T> newConnection(final Identifier destId) {
    if (myId == null)
      throw new RuntimeException("Trying to establish a connection from a Network Service that is not bound to any task");
    final Connection<T> conn = idToConnMap.get(destId);
    if (conn != null) {
      return conn;
    } else {
      final Connection<T> newConnection = new NSConnection<T>(myId, destId, new LinkListener<T>() {
        @Override
        public void messageReceived(Object message) {
        }
      }, this);

      final Connection<T> existing = idToConnMap.putIfAbsent(destId, newConnection);
      return (existing == null) ? newConnection : existing;
    }
  }

}

class MessageHandler<T> implements EventHandler<TransportEvent> {

  private final EventHandler<Message<T>> handler;
  private final NSMessageCodec<T> codec;

  public MessageHandler(final EventHandler<Message<T>> handler, final Codec<T> codec, final IdentifierFactory factory) {
    this.handler = handler;
    this.codec = new NSMessageCodec<T>(codec, factory);
  }

  @Override
  public void onNext(final TransportEvent value) {
    final byte[] data = value.getData();
    final NSMessage<T> obj = codec.decode(data);
    handler.onNext(obj);
  }

}
