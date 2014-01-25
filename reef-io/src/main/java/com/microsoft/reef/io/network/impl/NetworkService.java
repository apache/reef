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
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.Stage;
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
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Network service for Activity
 */
public class NetworkService<T> implements Stage, ConnectionFactory<T> {

  private static final Logger LOG = Logger.getLogger(NetworkService.class.getName());

  private final Identifier myId;
  private final IdentifierFactory factory;
  private final Codec<T> codec;
  private final Transport transport;
  private final NameClient nameClient;

  private final ConcurrentMap<Identifier, Connection<T>> idToConnMap = new ConcurrentHashMap<Identifier, Connection<T>>();

  @Inject
  public NetworkService(
      //@Parameter(ActivityConfiguration.Identifier.class) String myId,
      @Parameter(NetworkServiceParameters.ActivityId.class) String myId,
      @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) IdentifierFactory factory,
      @Parameter(NetworkServiceParameters.NetworkServicePort.class) int nsPort,
      @Parameter(NameServerParameters.NameServerAddr.class) String nameServerAddr,
      @Parameter(NameServerParameters.NameServerPort.class) int nameServerPort,
      @Parameter(NetworkServiceParameters.NetworkServiceCodec.class) Codec<T> codec,
      @Parameter(NetworkServiceParameters.NetworkServiceTransportFactory.class) TransportFactory tpFactory,
      @Parameter(NetworkServiceParameters.NetworkServiceHandler.class) EventHandler<Message<T>> recvHandler,
      @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) EventHandler<Exception> exHandler) {

    this.myId = factory.getNewInstance(myId);
    this.factory = factory;
    this.codec = codec;
    this.transport = tpFactory.create(nsPort, new LoggingEventHandler<TransportEvent>(),
        new MessageHandler<T>(recvHandler, codec, factory), exHandler);
    this.nameClient = new NameClient(nameServerAddr, nameServerPort, factory, new NameCache(30000));
    if (nsPort == 0) {
      nsPort = transport.getListeningPort();
      final CountDownLatch registered = new CountDownLatch(1);
      SingleThreadStage<Tuple<Identifier, InetSocketAddress>> stage = new SingleThreadStage<>("NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {

        @Override
        public void onNext(Tuple<Identifier, InetSocketAddress> tuple) {

          try {
            nameClient.register(tuple.getKey(), tuple.getValue());
            registered.countDown();
            LOG.fine("Finished nameservice registration");
            System.out.println("Finished nameservice registration");
          } catch (Exception e) {
            throw new RuntimeException("Unable to register with name service", e);
          }
        }
      }, 5);

      final Tuple<Identifier, InetSocketAddress> tuple = new Tuple<>(getMyId(), (InetSocketAddress) transport.getLocalAddress());
      stage.onNext(tuple);
      try {
        LOG.log(Level.FINE, "Waiting for nameservice registration");
        System.out.println("Waiting for nameservice registration");
        registered.await();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for name service registration", e);
      }
    }
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
