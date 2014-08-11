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
package com.microsoft.reef.io.network.naming;

import com.microsoft.reef.io.naming.NamingRegistry;
import com.microsoft.reef.io.network.naming.exception.NamingException;
import com.microsoft.reef.io.network.naming.serialization.NamingMessage;
import com.microsoft.reef.io.network.naming.serialization.NamingRegisterRequest;
import com.microsoft.reef.io.network.naming.serialization.NamingRegisterResponse;
import com.microsoft.reef.io.network.naming.serialization.NamingUnregisterRequest;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.Stage;
import com.microsoft.wake.impl.SyncStage;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.LinkListener;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.LoggingLinkListener;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming registry client
 */
public class NameRegistryClient implements Stage, NamingRegistry {

  private static final Logger LOG = Logger.getLogger(NameRegistryClient.class.getName());

  private final SocketAddress serverSocketAddr;
  private final Transport transport;
  private final Codec<NamingMessage> codec;
  private final BlockingQueue<NamingRegisterResponse> replyQueue;
  private final long timeout;

  /**
   * Constructs a naming registry client
   *
   * @param serverAddr a name server address
   * @param serverPort a name server port
   * @param factory an identifier factory
   */
  public NameRegistryClient(
      final String serverAddr, final int serverPort, final IdentifierFactory factory) {
    this(serverAddr, serverPort, 10000, factory);
  }

  /**
   * Constructs a naming registry client
   *
   * @param serverAddr a name server address
   * @param serverPort a name server port
   * @param timeout timeout in ms
   * @param factory an identifier factory
   */
  public NameRegistryClient(final String serverAddr, final int serverPort,
                            final long timeout, final IdentifierFactory factory) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.codec = NamingCodecFactory.createRegistryCodec(factory);
    this.replyQueue = new LinkedBlockingQueue<>();
    this.transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), 0,
        new SyncStage<>(new NamingRegistryClientHandler(new NamingRegistryResponseHandler(replyQueue), codec)),
        null, 3, 10000);
  }

  public NameRegistryClient(final String serverAddr, final int serverPort,
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
   * Registers an (identifier, address) mapping
   *
   * @param id an identifier
   * @param addr an Internet socket address
   */
  @Override
  public void register(final Identifier id, final InetSocketAddress addr) throws Exception {

    // needed to keep threads from reading the wrong response
    // TODO: better fix matches replies to threads with a map after REEF-198
    synchronized (this) {

      LOG.log(Level.FINE, "Register {0} : {1}", new Object[] { id, addr });

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
   * Unregisters an identifier
   *
   * @param id an identifier
   */
  @Override
  public void unregister(Identifier id) throws IOException {
    Link<NamingMessage> link = transport.open(serverSocketAddr, codec,
        new LinkListener<NamingMessage>() {
          @Override
          public void messageReceived(NamingMessage message) {
          }
        });
    link.write(new NamingUnregisterRequest(id));
  }

  /**
   * Closes resources
   */
  @Override
  public void close() throws Exception {
    // Should not close transport as we did not
    // create it
  }
}

/**
 * Naming registry client transport event handler
 */
class NamingRegistryClientHandler implements EventHandler<TransportEvent> {
  private static final Logger LOG = Logger.getLogger(NamingRegistryClientHandler.class.getName());

  private final EventHandler<NamingRegisterResponse> handler;
  private final Codec<NamingMessage> codec;

  NamingRegistryClientHandler(EventHandler<NamingRegisterResponse> handler, Codec<NamingMessage> codec) {
    this.handler = handler;
    this.codec = codec;
  }

  @Override
  public void onNext(TransportEvent value) {
    LOG.log(Level.FINE, value.toString());
    handler.onNext((NamingRegisterResponse)codec.decode(value.getData()));
  }
}

/**
 * Naming register response handler
 */
class NamingRegistryResponseHandler implements EventHandler<NamingRegisterResponse> {

  private final BlockingQueue<NamingRegisterResponse> replyQueue;

  NamingRegistryResponseHandler(BlockingQueue<NamingRegisterResponse> replyQueue) {
    this.replyQueue = replyQueue;
  }

  @Override
  public void onNext(NamingRegisterResponse value) {
    replyQueue.offer(value);
  }
}
