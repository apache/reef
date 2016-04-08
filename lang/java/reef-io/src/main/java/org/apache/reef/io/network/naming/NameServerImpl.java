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
package org.apache.reef.io.network.naming;

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.network.naming.serialization.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.apache.reef.webserver.ReefEventStateManager;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming server implementation.
 */
public final class NameServerImpl implements NameServer {

  private static final Logger LOG = Logger.getLogger(NameServer.class.getName());

  private final Transport transport;
  private final Map<Identifier, InetSocketAddress> idToAddrMap;
  private final ReefEventStateManager reefEventStateManager;
  private final int port;
  private final LocalAddressProvider localAddressProvider;

  /**
   * @param port    a listening port number
   * @param factory an identifier factory
   * @param localAddressProvider a local address provider
   * Constructs a name server
   */
  @Inject
  private NameServerImpl(
      @Parameter(NameServerParameters.NameServerPort.class) final int port,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory factory,
      final LocalAddressProvider localAddressProvider) {

    final Injector injector = Tang.Factory.getTang().newInjector();

    this.localAddressProvider = localAddressProvider;
    this.reefEventStateManager = null;
    final Codec<NamingMessage> codec = NamingCodecFactory.createFullCodec(factory);
    final EventHandler<NamingMessage> handler = createEventHandler(codec);

    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, localAddressProvider.getLocalAddress());
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, port);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class,
        new SyncStage<>(new NamingServerHandler(handler, codec)));

    try {
      this.transport = injector.getInstance(NettyMessagingTransport.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    this.port = transport.getListeningPort();
    this.idToAddrMap = Collections.synchronizedMap(new HashMap<Identifier, InetSocketAddress>());

    LOG.log(Level.FINE, "NameServer starting, listening at port {0}", this.port);
  }

  private EventHandler<NamingMessage> createEventHandler(final Codec<NamingMessage> codec) {

    final Map<Class<? extends NamingMessage>, EventHandler<? extends NamingMessage>>
        clazzToHandlerMap = new HashMap<>();

    clazzToHandlerMap.put(NamingLookupRequest.class, new NamingLookupRequestHandler(this, codec));
    clazzToHandlerMap.put(NamingRegisterRequest.class, new NamingRegisterRequestHandler(this, codec));
    clazzToHandlerMap.put(NamingUnregisterRequest.class, new NamingUnregisterRequestHandler(this));
    final EventHandler<NamingMessage> handler = new MultiEventHandler<>(clazzToHandlerMap);

    return handler;
  }
  /**
   * Gets port.
   */
  @Override
  public int getPort() {
    return port;
  }

  /**
   * Closes resources.
   */
  @Override
  public void close() throws Exception {
    transport.close();
  }

  /**
   * Registers an (identifier, address) mapping locally.
   *
   * @param id   an identifier
   * @param addr an Internet socket address
   */
  @Override
  public void register(final Identifier id, final InetSocketAddress addr) {
    LOG.log(Level.FINE, "id: " + id + " addr: " + addr);
    idToAddrMap.put(id, addr);
  }

  /**
   * Unregisters an identifier locally.
   *
   * @param id an identifier
   */
  @Override
  public void unregister(final Identifier id) {
    LOG.log(Level.FINE, "id: " + id);
    idToAddrMap.remove(id);
  }

  /**
   * Finds an address for an identifier locally.
   *
   * @param id an identifier
   * @return an Internet socket address
   */
  @Override
  public InetSocketAddress lookup(final Identifier id) {
    LOG.log(Level.FINE, "id: {0}", id);
    return idToAddrMap.get(id);
  }

  /**
   * Finds addresses for identifiers locally.
   *
   * @param identifiers an iterable of identifiers
   * @return a list of name assignments
   */
  @Override
  public List<NameAssignment> lookup(final Iterable<Identifier> identifiers) {
    LOG.log(Level.FINE, "identifiers");
    final List<NameAssignment> nas = new ArrayList<>();
    for (final Identifier id : identifiers) {
      final InetSocketAddress addr = idToAddrMap.get(id);
      LOG.log(Level.FINEST, "id : {0} addr: {1}", new Object[]{id, addr});
      if (addr != null) {
        nas.add(new NameAssignmentTuple(id, addr));
      }
    }
    return nas;
  }

  private String getNameServerId() {
    return this.localAddressProvider.getLocalAddress() + ":" + getPort();
  }
}

/**
 * Naming server transport event handler that invokes a specific naming message handler.
 */
class NamingServerHandler implements EventHandler<TransportEvent> {

  private final Codec<NamingMessage> codec;
  private final EventHandler<NamingMessage> handler;

  NamingServerHandler(final EventHandler<NamingMessage> handler, final Codec<NamingMessage> codec) {
    this.codec = codec;
    this.handler = handler;
  }

  @Override
  public void onNext(final TransportEvent value) {
    final byte[] data = value.getData();
    final NamingMessage message = codec.decode(data);
    message.setLink(value.getLink());
    handler.onNext(message);
  }
}

/**
 * Naming lookup request handler.
 */
class NamingLookupRequestHandler implements EventHandler<NamingLookupRequest> {

  private static final Logger LOG = Logger.getLogger(NamingLookupRequestHandler.class.getName());


  private final NameServer server;
  private final Codec<NamingMessage> codec;

  NamingLookupRequestHandler(final NameServer server, final Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }

  @Override
  public void onNext(final NamingLookupRequest value) {
    final List<NameAssignment> nas = server.lookup(value.getIdentifiers());
    final byte[] resp = codec.encode(new NamingLookupResponse(nas));
    value.getLink().write(resp);
  }
}

/**
 * Naming register request handler.
 */
class NamingRegisterRequestHandler implements EventHandler<NamingRegisterRequest> {

  private static final Logger LOG = Logger.getLogger(NamingRegisterRequestHandler.class.getName());


  private final NameServer server;
  private final Codec<NamingMessage> codec;

  NamingRegisterRequestHandler(final NameServer server, final Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }

  @Override
  public void onNext(final NamingRegisterRequest value) {
    server.register(value.getNameAssignment().getIdentifier(), value.getNameAssignment().getAddress());
    final byte[] resp = codec.encode(new NamingRegisterResponse(value));
    value.getLink().write(resp);
  }
}

/**
 * Naming unregister request handler.
 */
class NamingUnregisterRequestHandler implements EventHandler<NamingUnregisterRequest> {

  private final NameServer server;

  NamingUnregisterRequestHandler(final NameServer server) {
    this.server = server;
  }

  @Override
  public void onNext(final NamingUnregisterRequest value) {
    server.unregister(value.getIdentifier());
  }
}