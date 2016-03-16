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

import org.apache.reef.io.naming.NamingRegistry;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.io.network.naming.serialization.NamingMessage;
import org.apache.reef.io.network.naming.serialization.NamingRegisterRequest;
import org.apache.reef.io.network.naming.serialization.NamingRegisterResponse;
import org.apache.reef.io.network.naming.serialization.NamingUnregisterRequest;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming registry client.
 */
public class NameRegistryClient implements Stage, NamingRegistry {

  private static final Logger LOG = Logger.getLogger(NameRegistryClient.class.getName());

  private final SocketAddress serverSocketAddr;
  private final Transport transport;
  private final Codec<NamingMessage> codec;
  private final BlockingQueue<NamingRegisterResponse> replyQueue;
  private final long timeout;

  /**
   * Constructs a naming registry client.
   *
   * @param serverAddr a name server address
   * @param serverPort a name server port
   * @param factory    an identifier factory
   */
  NameRegistryClient(
      final String serverAddr, final int serverPort, final IdentifierFactory factory,
      final LocalAddressProvider localAddressProvider) {
    this(serverAddr, serverPort, 10000, factory, localAddressProvider);
  }

  /**
   * Constructs a naming registry client.
   *
   * @param serverAddr a name server address
   * @param serverPort a name server port
   * @param timeout    timeout in ms
   * @param factory    an identifier factory
   */
  NameRegistryClient(final String serverAddr,
                            final int serverPort,
                            final long timeout,
                            final IdentifierFactory factory,
                            final LocalAddressProvider localAddressProvider) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.codec = NamingCodecFactory.createRegistryCodec(factory);
    this.replyQueue = new LinkedBlockingQueue<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, localAddressProvider.getLocalAddress());
    injector.bindVolatileParameter(RemoteConfiguration.RemoteClientStage.class,
        new SyncStage<>(new NamingRegistryClientHandler(new NamingRegistryResponseHandler(replyQueue), codec)));

    try {
      this.transport = injector.getInstance(NettyMessagingTransport.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  NameRegistryClient(final String serverAddr, final int serverPort,
                            final long timeout, final IdentifierFactory factory,
                            final BlockingQueue<NamingRegisterResponse> replyQueue,
                            final Transport transport) {
    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.codec = NamingCodecFactory.createFullCodec(factory);
    this.replyQueue = replyQueue;
    this.transport = transport;
  }

  /**
   * Registers an (identifier, address) mapping.
   *
   * @param id   an identifier
   * @param addr an Internet socket address
   */
  @Override
  public void register(final Identifier id, final InetSocketAddress addr) throws Exception {

    // needed to keep threads from reading the wrong response
    // TODO: better fix matches replies to threads with a map after REEF-198
    synchronized (this) {

      LOG.log(Level.FINE, "Register {0} : {1}", new Object[]{id, addr});

      final Link<NamingMessage> link = this.transport.open(
          this.serverSocketAddr, this.codec, new LoggingLinkListener<NamingMessage>());

      link.write(new NamingRegisterRequest(new NameAssignmentTuple(id, addr)));

      for (;;) {
        try {
          this.replyQueue.poll(this.timeout, TimeUnit.MILLISECONDS);
          break;
        } catch (final InterruptedException e) {
          LOG.log(Level.INFO, "Interrupted", e);
          throw new NamingException(e);
        }
      }
    }
  }

  /**
   * Unregisters an identifier.
   *
   * @param id an identifier
   */
  @Override
  public void unregister(final Identifier id) throws IOException {
    final Link<NamingMessage> link = transport.open(serverSocketAddr, codec,
        new LoggingLinkListener<NamingMessage>());
    link.write(new NamingUnregisterRequest(id));
  }

  /**
   * Closes resources.
   */
  @Override
  public void close() throws Exception {
    // Should not close transport as we did not
    // create it
  }
}

/**
 * Naming registry client transport event handler.
 */
class NamingRegistryClientHandler implements EventHandler<TransportEvent> {
  private static final Logger LOG = Logger.getLogger(NamingRegistryClientHandler.class.getName());

  private final EventHandler<NamingRegisterResponse> handler;
  private final Codec<NamingMessage> codec;

  NamingRegistryClientHandler(final EventHandler<NamingRegisterResponse> handler, final Codec<NamingMessage> codec) {
    this.handler = handler;
    this.codec = codec;
  }

  @Override
  public void onNext(final TransportEvent value) {
    LOG.log(Level.FINE, value.toString());
    handler.onNext((NamingRegisterResponse) codec.decode(value.getData()));
  }
}

/**
 * Naming register response handler.
 */
class NamingRegistryResponseHandler implements EventHandler<NamingRegisterResponse> {
  private static final Logger LOG = Logger.getLogger(NamingRegistryResponseHandler.class.getName());

  private final BlockingQueue<NamingRegisterResponse> replyQueue;

  NamingRegistryResponseHandler(final BlockingQueue<NamingRegisterResponse> replyQueue) {
    this.replyQueue = replyQueue;
  }

  @Override
  public void onNext(final NamingRegisterResponse value) {
    if (!replyQueue.offer(value)) {
      LOG.log(Level.FINEST, "Element {0} was not added to the queue", value);
    }
  }
}
