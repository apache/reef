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
package org.apache.reef.io.network.naming;

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.network.naming.serialization.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.apache.reef.webserver.AvroReefServiceInfo;
import org.apache.reef.webserver.ReefEventStateManager;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming server implementation
 */
public class NameServerImpl implements NameServer {

  private static final Logger LOG = Logger.getLogger(NameServer.class.getName());

  private final Transport transport;
  private final Map<Identifier, InetSocketAddress> idToAddrMap;
  private final ReefEventStateManager reefEventStateManager;
  private final int port;

  /**
   * @param port    a listening port number
   * @param factory an identifier factory
   * @deprecated inject the NameServer instead of new it up
   * Constructs a name server
   */
  // TODO: All existing NameServer usage is currently new-up, need to make them injected as well.
  @Deprecated
  public NameServerImpl(
      final int port,
      final IdentifierFactory factory) {

    this.reefEventStateManager = null;
    final Codec<NamingMessage> codec = NamingCodecFactory.createFullCodec(factory);
    final EventHandler<NamingMessage> handler = createEventHandler(codec);

    this.transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), port, null,
        new SyncStage<>(new NamingServerHandler(handler, codec)), 3, 10000);

    this.port = transport.getListeningPort();
    this.idToAddrMap = Collections.synchronizedMap(new HashMap<Identifier, InetSocketAddress>());

    LOG.log(Level.FINE, "NameServer starting, listening at port {0}", this.port);
  }


  /**
   * Constructs a name server
   *
   * @param port                  a listening port number
   * @param factory               an identifier factory
   * @param reefEventStateManager the event state manager used to register name server info
   */
  @Inject
  public NameServerImpl(
      final @Parameter(NameServerParameters.NameServerPort.class) int port,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory factory,
      final ReefEventStateManager reefEventStateManager) {

    this.reefEventStateManager = reefEventStateManager;
    final Codec<NamingMessage> codec = NamingCodecFactory.createFullCodec(factory);
    final EventHandler<NamingMessage> handler = createEventHandler(codec);

    this.transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), port, null,
        new SyncStage<>(new NamingServerHandler(handler, codec)), 3, 10000);

    this.port = transport.getListeningPort();
    this.idToAddrMap = Collections.synchronizedMap(new HashMap<Identifier, InetSocketAddress>());

    this.reefEventStateManager.registerServiceInfo(
        AvroReefServiceInfo.newBuilder()
            .setServiceName("NameServer")
            .setServiceInfo(getNameServerId())
            .build());
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
   * Gets port
   */
  @Override
  public int getPort() {
    return port;
  }

  /**
   * Closes resources
   */
  @Override
  public void close() throws Exception {
    transport.close();
  }

  /**
   * Registers an (identifier, address) mapping locally
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
   * Unregisters an identifier locally
   *
   * @param id an identifier
   */
  @Override
  public void unregister(final Identifier id) {
    LOG.log(Level.FINE, "id: " + id);
    idToAddrMap.remove(id);
  }

  /**
   * Finds an address for an identifier locally
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
   * Finds addresses for identifiers locally
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
    return NetUtils.getLocalAddress() + ":" + getPort();
  }
}

/**
 * Naming server transport event handler that invokes a specific naming message handler
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
 * Naming lookup request handler
 */
class NamingLookupRequestHandler implements EventHandler<NamingLookupRequest> {

  private static final Logger LOG = Logger.getLogger(NamingLookupRequestHandler.class.getName());


  private final NameServer server;
  private final Codec<NamingMessage> codec;

  public NamingLookupRequestHandler(final NameServer server, final Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }

  @Override
  public void onNext(final NamingLookupRequest value) {
    final List<NameAssignment> nas = server.lookup(value.getIdentifiers());
    final byte[] resp = codec.encode(new NamingLookupResponse(nas));
    try {
      value.getLink().write(resp);
    } catch (final IOException e) {
      //Actually, there is no way Link.write can throw and IOException
      //after netty4 merge. This needs to cleaned up
      LOG.throwing("NamingLookupRequestHandler", "onNext", e);
    }
  }
}

/**
 * Naming register request handler
 */
class NamingRegisterRequestHandler implements EventHandler<NamingRegisterRequest> {

  private static final Logger LOG = Logger.getLogger(NamingRegisterRequestHandler.class.getName());


  private final NameServer server;
  private final Codec<NamingMessage> codec;

  public NamingRegisterRequestHandler(final NameServer server, final Codec<NamingMessage> codec) {
    this.server = server;
    this.codec = codec;
  }

  @Override
  public void onNext(final NamingRegisterRequest value) {
    server.register(value.getNameAssignment().getIdentifier(), value.getNameAssignment().getAddress());
    final byte[] resp = codec.encode(new NamingRegisterResponse(value));
    try {
      value.getLink().write(resp);
    } catch (final IOException e) {
      //Actually, there is no way Link.write can throw and IOException
      //after netty4 merge. This needs to cleaned up
      LOG.throwing("NamingRegisterRequestHandler", "onNext", e);
    }
  }
}

/**
 * Naming unregister request handler
 */
class NamingUnregisterRequestHandler implements EventHandler<NamingUnregisterRequest> {

  private final NameServer server;

  public NamingUnregisterRequestHandler(final NameServer server) {
    this.server = server;
  }

  @Override
  public void onNext(final NamingUnregisterRequest value) {
    server.unregister(value.getIdentifier());
  }
}