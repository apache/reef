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

import com.google.inject.Inject;
import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.naming.NamingLookup;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.io.network.naming.parameters.NameResolverCacheTimeout;
import org.apache.reef.io.network.naming.parameters.NameResolverIdentifierFactory;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.io.network.naming.serialization.NamingLookupRequest;
import org.apache.reef.io.network.naming.serialization.NamingLookupResponse;
import org.apache.reef.io.network.naming.serialization.NamingMessage;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.cache.Cache;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming lookup client.
 */
public class NameLookupClient implements Stage, NamingLookup {

  private static final Logger LOG = Logger.getLogger(NameLookupClient.class.getName());
  private final SocketAddress serverSocketAddr;
  private final Transport transport;
  private final Codec<NamingMessage> codec;
  private final BlockingQueue<NamingLookupResponse> replyQueue;
  private final long timeout;
  private final Cache<Identifier, InetSocketAddress> cache;
  private final int retryCount;
  private final int retryTimeout;

  /**
   * Constructs a naming lookup client.
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param factory    an identifier factory
   * @param cache      an cache
   *
   * @deprecated in 0.13. Have an instance injected instead or use the NameResolver interface.
   */
  @Deprecated
  public NameLookupClient(final String serverAddr,
                          final int serverPort,
                          final IdentifierFactory factory,
                          final int retryCount,
                          final int retryTimeout,
                          final Cache<Identifier, InetSocketAddress> cache,
                          final LocalAddressProvider localAddressProvider) {
    this(serverAddr, serverPort, 10000, factory, retryCount, retryTimeout, cache, localAddressProvider);
  }

  /**
   * Constructs a naming lookup client.
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param timeout    request timeout in ms
   * @param factory    an identifier factory
   * @param cache      an cache
   *
   * @deprecated in 0.13. Have an instance injected instead or use the NameResolver interface.
   */
  @Deprecated
  public NameLookupClient(final String serverAddr,
                          final int serverPort,
                          final long timeout,
                          final IdentifierFactory factory,
                          final int retryCount,
                          final int retryTimeout,
                          final Cache<Identifier, InetSocketAddress> cache,
                          final LocalAddressProvider localAddressProvider) {
    this(serverAddr, serverPort, timeout, factory, retryCount, retryTimeout,
        cache, localAddressProvider, new MessagingTransportFactory(localAddressProvider));
  }

  /**
   * Constructs a naming lookup client.
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param timeout    request timeout in ms
   * @param factory    an identifier factory
   * @param cache      an cache
   * @param tpFactory  a transport factory
   *
   * @deprecated in 0.13. Have an instance injected instead or use the NameResolver interface.
   */
  @Deprecated
  public NameLookupClient(final String serverAddr,
                          final int serverPort,
                          final long timeout,
                          final IdentifierFactory factory,
                          final int retryCount,
                          final int retryTimeout,
                          final Cache<Identifier, InetSocketAddress> cache,
                          final LocalAddressProvider localAddressProvider,
                          final TransportFactory tpFactory) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = cache;
    this.codec = NamingCodecFactory.createLookupCodec(factory);
    this.replyQueue = new LinkedBlockingQueue<>();

    this.transport = tpFactory.newInstance(localAddressProvider.getLocalAddress(), 0,
        new SyncStage<>(new NamingLookupClientHandler(
            new NamingLookupResponseHandler(this.replyQueue), this.codec)),
        null, retryCount, retryTimeout);

    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }

  /**
    * Constructs a naming lookup client.
    *
    * @param serverAddr a server address
    * @param serverPort a server port number
    * @param timeout    request timeout in ms
    * @param factory    an identifier factory
    * @param tpFactory  a transport factory
    */
  @Inject
  private NameLookupClient(
            @Parameter(NameResolverNameServerAddr.class) final String serverAddr,
            @Parameter(NameResolverNameServerPort.class) final int serverPort,
            @Parameter(NameResolverCacheTimeout.class) final long timeout,
            @Parameter(NameResolverIdentifierFactory.class) final IdentifierFactory factory,
            @Parameter(NameResolverRetryCount.class) final int retryCount,
            @Parameter(NameResolverRetryTimeout.class) final int retryTimeout,
            final LocalAddressProvider localAddressProvider,
            final TransportFactory tpFactory) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = new NameCache(timeout);
    this.codec = NamingCodecFactory.createLookupCodec(factory);
    this.replyQueue = new LinkedBlockingQueue<>();

    this.transport = tpFactory.newInstance(localAddressProvider.getLocalAddress(), 0,
            new SyncStage<>(new NamingLookupClientHandler(
                    new NamingLookupResponseHandler(this.replyQueue), this.codec)),
            null, retryCount, retryTimeout);

    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }

  /**
   * Constructs a naming lookup client.
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param timeout request timeout in ms
   * @param factory an identifier factory
   * @param retryCount number of retries
   * @param retryTimeout a timeout for a retry attempt, msec
   * @param replyQueue a queue of naming lookup responses
   * @param transport transport for sending and receiving data
   * @param cache a name cache
   *
   * @deprecated in 0.13. Have an instance injected instead or use the NameResolver interface.
   */
  @Deprecated
  NameLookupClient(final String serverAddr, final int serverPort, final long timeout,
                   final IdentifierFactory factory, final int retryCount, final int retryTimeout,
                   final BlockingQueue<NamingLookupResponse> replyQueue, final Transport transport,
                   final Cache<Identifier, InetSocketAddress> cache) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = cache;
    this.codec = NamingCodecFactory.createFullCodec(factory);
    this.replyQueue = replyQueue;
    this.transport = transport;
    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }



  /**
   * Finds an address for an identifier.
   *
   * @param id an identifier
   * @return an Internet socket address
   */
  @Override
  public InetSocketAddress lookup(final Identifier id) throws Exception {

    return cache.get(id, new Callable<InetSocketAddress>() {

      @Override
      public InetSocketAddress call() throws Exception {
        final int origRetryCount = NameLookupClient.this.retryCount;
        int retriesLeft = origRetryCount;
        while (true) {
          try {
            return remoteLookup(id);
          } catch (final NamingException e) {
            if (retriesLeft <= 0) {
              throw e;
            } else {
              final int currentRetryTimeout = NameLookupClient.this.retryTimeout
                  * (origRetryCount - retriesLeft + 1);
              LOG.log(Level.WARNING,
                  "Caught Naming Exception while looking up " + id
                      + " with Name Server. Will retry " + retriesLeft
                      + " time(s) after waiting for " + currentRetryTimeout + " msec.");
              Thread.sleep(currentRetryTimeout);
              --retriesLeft;
            }
          }
        }
      }

    });
  }

  /**
   * Retrieves an address for an identifier remotely.
   *
   * @param id an identifier
   * @return an Internet socket address
   * @throws Exception
   */
  public InetSocketAddress remoteLookup(final Identifier id) throws Exception {
    // the lookup is not thread-safe, because concurrent replies may
    // be read by the wrong thread.
    // TODO: better fix uses a map of id's after REEF-198
    synchronized (this) {

      LOG.log(Level.INFO, "Looking up {0} on NameServer {1}", new Object[]{id, serverSocketAddr});

      final List<Identifier> ids = Arrays.asList(id);
      final Link<NamingMessage> link = transport.open(serverSocketAddr, codec,
          new LoggingLinkListener<NamingMessage>());
      link.write(new NamingLookupRequest(ids));

      final NamingLookupResponse resp;
      for (;;) {
        try {
          resp = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
          break;
        } catch (final InterruptedException e) {
          LOG.log(Level.INFO, "Lookup interrupted", e);
          throw new NamingException(e);
        }
      }

      final List<NameAssignment> list = resp.getNameAssignments();
      if (list.isEmpty()) {
        throw new NamingException("Cannot find " + id + " from the name server");
      } else {
        return list.get(0).getAddress();
      }
    }
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
 * Naming lookup client transport event handler.
 */
class NamingLookupClientHandler implements EventHandler<TransportEvent> {

  private final EventHandler<NamingLookupResponse> handler;
  private final Codec<NamingMessage> codec;

  NamingLookupClientHandler(final EventHandler<NamingLookupResponse> handler, final Codec<NamingMessage> codec) {
    this.handler = handler;
    this.codec = codec;
  }

  @Override
  public void onNext(final TransportEvent value) {
    handler.onNext((NamingLookupResponse) codec.decode(value.getData()));
  }

}

/**
 * Naming lookup response handler.
 */
class NamingLookupResponseHandler implements EventHandler<NamingLookupResponse> {
  private static final Logger LOG = Logger.getLogger(NamingLookupResponseHandler.class.getName());

  private final BlockingQueue<NamingLookupResponse> replyQueue;

  NamingLookupResponseHandler(final BlockingQueue<NamingLookupResponse> replyQueue) {
    this.replyQueue = replyQueue;
  }

  @Override
  public void onNext(final NamingLookupResponse value) {
    if (!replyQueue.offer(value)) {
      LOG.log(Level.FINEST, "Element {0} was not added to the queue", value);
    }
  }
}
