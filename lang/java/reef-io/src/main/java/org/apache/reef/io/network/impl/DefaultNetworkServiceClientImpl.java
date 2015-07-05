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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkServiceClient;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default NetworkServiceClient implementation.
 */
public final class DefaultNetworkServiceClientImpl implements NetworkServiceClient {

  private static final Logger LOG = Logger.getLogger(DefaultNetworkServiceClientImpl.class.getName());

  private final IdentifierFactory idFactory;
  private final NameResolver nameResolver;
  private final Transport transport;
  private final EventHandler<TransportEvent> recvHandler;
  private final ConcurrentMap<String, NSConnectionFactory> connectionFactoryMap;
  private final ConcurrentMap<String, Boolean> isStreamingCodecMap;
  private Identifier myId;
  private final Codec<DefaultNSMessage> nsCodec;
  private final LinkListener<DefaultNSMessage> nsLinkListener;
  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  private final EStage<Identifier> nameServiceUnregisteringStage;

  @Inject
  private DefaultNetworkServiceClientImpl(
      @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(NetworkServiceParameters.NetworkServicePort.class) final int nsPort,
      final TransportFactory transportFactory,
      final NameResolver nameResolver) {

    this.idFactory = idFactory;
    this.connectionFactoryMap = new ConcurrentHashMap<>();
    this.isStreamingCodecMap = new ConcurrentHashMap<>();
    this.nsCodec = new DefaultNSMessageCodec(idFactory, connectionFactoryMap, isStreamingCodecMap);
    this.nsLinkListener = new NetworkServiceLinkListener(connectionFactoryMap);
    this.recvHandler = new NetworkServiceReceiveHandler(connectionFactoryMap, nsCodec);
    this.nameResolver = nameResolver;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler, new DefaultNSExceptionHandler());

    this.nameServiceRegisteringStage = new SingleThreadStage<>(
        "NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {
      @Override
      public void onNext(final Tuple<Identifier, InetSocketAddress> tuple) {
        try {
          nameResolver.register(tuple.getKey(), tuple.getValue());
          LOG.log(Level.FINEST, "Registered {0} with nameservice", tuple.getKey());
        } catch (final Exception ex) {
          final String msg = "Unable to register " + tuple.getKey() + " with name service";
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
          nameResolver.unregister(id);
          LOG.log(Level.FINEST, "Unregistered {0} with nameservice", id);
        } catch (final Exception ex) {
          final String msg = "Unable to unregister " + id + " with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);
  }

  @Override
  public synchronized <T> void registerConnectionFactory(final Class<? extends Name<String>> connectionFactoryId,
                                                         final Codec<T> codec,
                                                         final EventHandler<Message<T>> eventHandler,
                                                         final LinkListener<Message<T>> linkListener) throws NetworkException {

    final String connFactoryId = connectionFactoryId.getName();
    if (connectionFactoryMap.get(connFactoryId) != null) {
      throw new NetworkException("ConnectionFactory " + connectionFactoryId + " was already registered.");
    }

    connectionFactoryMap.put(connFactoryId, new NSConnectionFactory<>(this, connFactoryId, codec, eventHandler, linkListener));
    isStreamingCodecMap.put(connFactoryId, codec instanceof StreamingCodec);
  }

  @Override
  public synchronized void unregisterConnectionFactory(final Class<? extends Name<String>> connectionFactoryId) {
    final String connFactoryId = connectionFactoryId.getName();

    final ConnectionFactory  connFactory = connectionFactoryMap.get(connFactoryId);
    if (connFactory != null) {
      connectionFactoryMap.remove(connFactoryId);
      isStreamingCodecMap.remove(connFactoryId);
    }
  }

  /**
   * Registers identifier of NetworkService.
   * @param nsId
   * @throws Exception
   */
  @Override
  public void registerId(final Identifier nsId) {
    LOG.log(Level.INFO, "Registering NetworkSerice " + nsId);
    this.myId = nsId;
    final Tuple<Identifier, InetSocketAddress> tuple =
        new Tuple<>(nsId, (InetSocketAddress) this.transport.getLocalAddress());
    LOG.log(Level.FINEST, "Binding {0} to NetworkService@({1})",
        new Object[]{tuple.getKey(), tuple.getValue()});
    this.nameServiceRegisteringStage.onNext(tuple);
  }

  /**
   * Open a channel for remoteId.
   * @param remoteId
   * @throws NetworkException
   */
  <T> Link<DefaultNSMessage<T>> openLink(final Identifier remoteId) throws NetworkException {
    try {
      final SocketAddress address = nameResolver.lookup(remoteId);
      if (address == null) {
        throw new NetworkException("Lookup " + remoteId + " is null");
      }
      return transport.open(address, nsCodec, nsLinkListener);
    } catch(Exception e) {
      e.printStackTrace();
      throw new NetworkException(e);
    }
  }

  /**
   * Gets a ConnectionFactory.
   * @param connectionFactoryId the identifier of the ConnectionFActory
   */

  @Override
  public <T> ConnectionFactory<T> getConnectionFactory(final Class<? extends Name<String>> connectionFactoryId) {

    final ConnectionFactory<T> connFactory = connectionFactoryMap.get(connectionFactoryId.getName());

    if (connFactory == null) {
      throw new RuntimeException("Cannot find ConnectionFactory of " + connectionFactoryId + ".");
    }

    return connFactory;
  }

  @Override
  public void unregisterId(final Identifier nsId) {
    this.myId = null;
    LOG.log(Level.FINEST, "Unbinding {0} to NetworkService@({1})",
        new Object[]{nsId, this.transport.getLocalAddress()});
    this.nameServiceUnregisteringStage.onNext(nsId);
  }

  @Override
  public Identifier getNetworkServiceId() {
    return this.myId;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return transport.getLocalAddress();
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "Shutting down");
    this.nameServiceRegisteringStage.close();
    this.nameServiceUnregisteringStage.close();
    this.transport.close();
  }
}