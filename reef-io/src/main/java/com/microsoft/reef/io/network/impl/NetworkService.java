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

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
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
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.LinkListener;
import com.microsoft.wake.remote.transport.Transport;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network service for Task
 */
public class NetworkService<T> implements Stage, ConnectionFactory<T> {

  private static final Logger LOG = Logger.getLogger(NetworkService.class.getName());

  private final Identifier myId;
  private final IdentifierFactory factory;
  private final Codec<T> codec;
  private final Transport transport;
  private final NameClient nameClient;

  private ConcurrentMap<Identifier, Connection<T>> idToConnMap;

  @Inject
  public NetworkService(
      final @Parameter(TaskConfigurationOptions.Identifier.class) String myId,
      final @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) IdentifierFactory factory,
      final @Parameter(NetworkServiceParameters.NetworkServicePort.class) int nsPort,
      final @Parameter(NameServerParameters.NameServerAddr.class) String nameServerAddr,
      final @Parameter(NameServerParameters.NameServerPort.class) int nameServerPort,
      final @Parameter(NetworkServiceParameters.NetworkServiceCodec.class) Codec<T> codec,
      final @Parameter(NetworkServiceParameters.NetworkServiceTransportFactory.class) TransportFactory tpFactory,
      final @Parameter(NetworkServiceParameters.NetworkServiceHandler.class) EventHandler<Message<T>> recvHandler,
      final @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) EventHandler<Exception> exHandler) {

    this.myId = factory.getNewInstance(myId);
    this.factory = factory;
    this.codec = codec;
    this.transport = tpFactory.create(nsPort, new LoggingEventHandler<TransportEvent>(),
        new MessageHandler<T>(recvHandler, codec, factory), exHandler);
    this.nameClient = new NameClient(nameServerAddr, nameServerPort, factory, new NameCache(30000));
    this.idToConnMap = new ConcurrentHashMap<Identifier, Connection<T>>();
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

  public Naming getNameClinet() {
    return nameClient;
  }

  public IdentifierFactory getIdentifierFactory() {
    return factory;
  }

  void remove(Identifier id) {
    idToConnMap.remove(id);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.INFO, "Shutting down");
    transport.close();
    nameClient.close();
  }

  @Override
  public Connection<T> newConnection(Identifier destId) {
    Connection<T> conn;
    if ((conn = idToConnMap.get(destId)) != null)
      return conn;

    conn = new NSConnection<T>(myId, destId, new LinkListener<T>() {
      @Override
      public void messageReceived(Object message) {
      }
    }, this);

    Connection<T> existing = idToConnMap.putIfAbsent(destId, conn);
    return (existing == null) ? conn : existing;
  }
}

class MessageHandler<T> implements EventHandler<TransportEvent> {

  private final EventHandler<Message<T>> handler;
  private final NSMessageCodec<T> codec;

  public MessageHandler(EventHandler<Message<T>> handler, Codec<T> codec, IdentifierFactory factory) {
    this.handler = handler;
    this.codec = new NSMessageCodec<T>(codec, factory);
  }

  @Override
  public void onNext(TransportEvent value) {
    byte[] data = value.getData();
    NSMessage<T> obj = codec.decode(data);
    handler.onNext(obj);
  }
}
