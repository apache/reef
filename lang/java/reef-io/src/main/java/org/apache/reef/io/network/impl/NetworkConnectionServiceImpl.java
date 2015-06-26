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
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.io.network.impl.config.NetworkConnectionServicePort;
import org.apache.reef.io.network.naming.NameResolver;
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
 * Default Network connection service implementation.
 */
public final class NetworkConnectionServiceImpl implements NetworkConnectionService {

  private static final Logger LOG = Logger.getLogger(NetworkConnectionServiceImpl.class.getName());

  /**
   * An identifier factory registering network connection service id.
   */
  private final IdentifierFactory idFactory;
  /**
   * A name resolver looking up nameserver.
   */
  private final NameResolver nameResolver;
  /**
   * A messaging transport.
   */
  private final Transport transport;
  /**
   * A map of (id of connection factory, a connection factory instance).
   */
  private final ConcurrentMap<String, NetworkConnectionFactory> connFactoryMap;
  /**
   * A network connection service identifier.
   */
  private Identifier myId;
  /**
   * A network connection service message codec.
   */
  private final Codec<NetworkConnectionServiceMessage> nsCodec;
  /**
   * A network connection service link listener.
   */
  private final LinkListener<NetworkConnectionServiceMessage> nsLinkListener;
  /**
   * A stage registering identifiers to nameServer.
   */
  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  /**
   * A stage unregistering identifiers from nameServer.
   */
  private final EStage<Identifier> nameServiceUnregisteringStage;

  @Inject
  private NetworkConnectionServiceImpl(
      @Parameter(NetworkConnectionServiceIdFactory.class) final IdentifierFactory idFactory,
      @Parameter(NetworkConnectionServicePort.class) final int nsPort,
      final TransportFactory transportFactory,
      final NameResolver nameResolver) {
    this.idFactory = idFactory;
    this.connFactoryMap = new ConcurrentHashMap<>();
    this.nsCodec = new NetworkConnectionServiceMessageCodec(idFactory, connFactoryMap);
    this.nsLinkListener = new NetworkConnectionServiceLinkListener(connFactoryMap);
    final EventHandler<TransportEvent> recvHandler =
        new NetworkConnectionServiceReceiveHandler(connFactoryMap, nsCodec);
    this.nameResolver = nameResolver;
    this.transport = transportFactory.newInstance(nsPort, recvHandler, recvHandler,
        new NetworkConnectionServiceExceptionHandler());

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
  public <T> void registerConnectionFactory(final Identifier connFactoryId,
                                            final Codec<T> codec,
                                            final EventHandler<Message<T>> eventHandler,
                                            final LinkListener<Message<T>> linkListener) throws NetworkException {
    String id = connFactoryId.toString();
    if (connFactoryMap.get(id) != null) {
      throw new NetworkException("ConnectionFactory " + connFactoryId + " was already registered.");
    }
    final ConnectionFactory connFactory = connFactoryMap.putIfAbsent(id,
        new NetworkConnectionFactory<>(this, id, codec, eventHandler, linkListener));

    if (connFactory != null) {
      throw new NetworkException("ConnectionFactory " + connFactoryId + " was already registered.");
    }
  }

  @Override
  public void unregisterConnectionFactory(final Identifier connFactoryId) {
    final String id = connFactoryId.toString();
    final ConnectionFactory  connFactory = connFactoryMap.get(id);
    if (connFactory != null) {
      final ConnectionFactory cf = connFactoryMap.remove(id);
      if (cf == null) {
        LOG.log(Level.WARNING, "ConnectionFactory of {0} is null", id);
      }
    } else {
      LOG.log(Level.WARNING, "ConnectionFactory of {0} is null", id);
    }
  }

  /**
   * Registers a source identifier of NetworkConnectionService.
   * @param ncsId
   * @throws Exception
   */
  @Override
  public void registerId(final Identifier ncsId) {
    LOG.log(Level.INFO, "Registering NetworkConnectionService " + ncsId);
    this.myId = ncsId;
    final Tuple<Identifier, InetSocketAddress> tuple =
        new Tuple<>(ncsId, (InetSocketAddress) this.transport.getLocalAddress());
    LOG.log(Level.FINEST, "Binding {0} to NetworkConnectionService@({1})",
        new Object[]{tuple.getKey(), tuple.getValue()});
    this.nameServiceRegisteringStage.onNext(tuple);
  }

  /**
   * Open a channel for destination identifier of NetworkConnectionService.
   * @param destId
   * @throws NetworkException
   */
  <T> Link<NetworkConnectionServiceMessage<T>> openLink(final Identifier destId) throws NetworkException {
    try {
      final SocketAddress address = nameResolver.lookup(destId);
      if (address == null) {
        throw new NetworkException("Lookup " + destId + " is null");
      }
      return transport.open(address, nsCodec, nsLinkListener);
    } catch(Exception e) {
      e.printStackTrace();
      throw new NetworkException(e);
    }
  }

  /**
   * Gets a ConnectionFactory.
   * @param connFactoryId the identifier of the ConnectionFactory
   */
  @Override
  public <T> ConnectionFactory<T> getConnectionFactory(final Identifier connFactoryId) {
    final ConnectionFactory<T> connFactory = connFactoryMap.get(connFactoryId.toString());
    if (connFactory == null) {
      throw new RuntimeException("Cannot find ConnectionFactory of " + connFactoryId + ".");
    }
    return connFactory;
  }

  @Override
  public void unregisterId(final Identifier ncsId) {
    LOG.log(Level.FINEST, "Unbinding {0} to NetworkConnectionService@({1})",
        new Object[]{ncsId, this.transport.getLocalAddress()});
    this.myId = null;
    this.nameServiceUnregisteringStage.onNext(ncsId);
  }

  @Override
  public Identifier getNetworkConnectionServiceId() {
    return this.myId;
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "Shutting down");
    this.nameServiceRegisteringStage.close();
    this.nameServiceUnregisteringStage.close();
    this.transport.close();
  }
}