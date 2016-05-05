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
package org.apache.reef.io.network.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
  /**
   * A boolean flag that indicates whether the NetworkConnectionService is closed.
   */
  private final AtomicBoolean isClosed;
  /**
   * A DELIMITER to make a concatenated end point id {{connectionFactoryId}}{{DELIMITER}}{{localEndPointId}}.
   */
  private static final String DELIMITER = "/";

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

    this.isClosed = new AtomicBoolean();
  }


  private void checkBeforeRegistration(final String connectionFactoryId) {
    if (isClosed.get()) {
      throw new NetworkRuntimeException("Unable to register new ConnectionFactory to closed NetworkConnectionService");
    }

    if (connFactoryMap.get(connectionFactoryId) != null) {
      throw new NetworkRuntimeException("ConnectionFactory " + connectionFactoryId + " was already registered.");
    }

    if (connectionFactoryId.contains(DELIMITER)) {
      throw new NetworkRuntimeException(
          "The ConnectionFactoryId " + connectionFactoryId + " should not contain " + DELIMITER);
    }
  }

  @Override
  public <T> ConnectionFactory<T> registerConnectionFactory(
      final Identifier connectionFactoryId,
      final Codec<T> codec,
      final EventHandler<Message<T>> eventHandler,
      final LinkListener<Message<T>> linkListener,
      final Identifier localEndPointId) {
    final String id = connectionFactoryId.toString();
    checkBeforeRegistration(id);

    final NetworkConnectionFactory<T> connectionFactory = new NetworkConnectionFactory<>(
        this, connectionFactoryId, codec, eventHandler, linkListener, localEndPointId);
    final Identifier localId = getEndPointIdWithConnectionFactoryId(connectionFactoryId, localEndPointId);
    nameServiceRegisteringStage.onNext(new Tuple<>(localId, (InetSocketAddress) transport.getLocalAddress()));

    if (connFactoryMap.putIfAbsent(id, connectionFactory) != null) {
      throw new NetworkRuntimeException("ConnectionFactory " + connectionFactoryId + " was already registered.");
    }

    LOG.log(Level.INFO, "ConnectionFactory {0} was registered", id);

    return connectionFactory;
  }

  @Override
  public void unregisterConnectionFactory(final Identifier connFactoryId) {
    final String id = connFactoryId.toString();
    final NetworkConnectionFactory connFactory = connFactoryMap.remove(id);
    if (connFactory != null) {
      LOG.log(Level.INFO, "ConnectionFactory {0} was unregistered", id);

      final Identifier localId = getEndPointIdWithConnectionFactoryId(
            connFactoryId, connFactory.getLocalEndPointId());
      nameServiceUnregisteringStage.onNext(localId);
    } else {
      LOG.log(Level.WARNING, "ConnectionFactory of {0} is null", id);
    }
  }


  /**
   * Open a channel for destination identifier of NetworkConnectionService.
   * @param connectionFactoryId
   * @param remoteEndPointId
   * @throws NetworkException
   */
  <T> Link<NetworkConnectionServiceMessage<T>> openLink(
      final Identifier connectionFactoryId, final Identifier remoteEndPointId) throws NetworkException {
    final Identifier remoteId = getEndPointIdWithConnectionFactoryId(connectionFactoryId, remoteEndPointId);
    try {
      final SocketAddress address = nameResolver.lookup(remoteId);
      if (address == null) {
        throw new NetworkException("Lookup " + remoteId + " is null");
      }
      return transport.open(address, nsCodec, nsLinkListener);
    } catch(final Exception e) {
      throw new NetworkException(e);
    }
  }


  private Identifier getEndPointIdWithConnectionFactoryId(
      final Identifier connectionFactoryId, final Identifier endPointId) {
    final String identifier = connectionFactoryId.toString() + DELIMITER + endPointId.toString();
    return idFactory.getNewInstance(identifier);
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
  public void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      LOG.log(Level.FINE, "Shutting down");
      this.nameServiceRegisteringStage.close();
      this.nameServiceUnregisteringStage.close();
      this.nameResolver.close();
      this.transport.close();
    }
  }
}
