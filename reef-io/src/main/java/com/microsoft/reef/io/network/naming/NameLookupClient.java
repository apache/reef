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

import com.microsoft.reef.io.naming.NameAssignment;
import com.microsoft.reef.io.naming.NamingLookup;
import com.microsoft.reef.io.network.Cache;
import com.microsoft.reef.io.network.naming.exception.NamingException;
import com.microsoft.reef.io.network.naming.serialization.NamingLookupRequest;
import com.microsoft.reef.io.network.naming.serialization.NamingLookupResponse;
import com.microsoft.reef.io.network.naming.serialization.NamingMessage;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.Stage;
import com.microsoft.wake.impl.SyncStage;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.LoggingLinkListener;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

/**
 * Naming lookup client
 */
public class NameLookupClient implements Stage, NamingLookup {

  private static final Logger LOG = Logger.getLogger(NameLookupClient.class.getName());

  @NamedParameter(doc="When should a retry timeout(msec)?", short_name="retryTimeout", default_value="100")
  public static class RetryTimeout implements Name<Integer> {}

  @NamedParameter(doc="How many times should I retry?", short_name="retryCount", default_value="10")
  public static class RetryCount implements Name<Integer> {}

  private final SocketAddress serverSocketAddr;
  private final Transport transport;
  private final Codec<NamingMessage> codec;
  private final BlockingQueue<NamingLookupResponse> replyQueue;
  private final long timeout;
  private final Cache<Identifier, InetSocketAddress> cache;
  private final int retryCount;
  private final int retryTimeout;

  /**
   * Constructs a naming lookup client
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param factory an identifier factory
   * @param cache an cache
   */
  public NameLookupClient(final String serverAddr, final int serverPort,
      final IdentifierFactory factory, final int retryCount, final int retryTimeout,
      final Cache<Identifier, InetSocketAddress> cache) {
    this(serverAddr, serverPort, 10000, factory, retryCount, retryTimeout, cache);
  }

  /**
   * Constructs a naming lookup client
   *
   * @param serverAddr a server address
   * @param serverPort a server port number
   * @param timeout request timeout in ms
   * @param factory an identifier factory
   * @param cache an cache
   */
  public NameLookupClient(final String serverAddr, final int serverPort, final long timeout,
      final IdentifierFactory factory, final int retryCount, final int retryTimeout,
      final Cache<Identifier, InetSocketAddress> cache) {

    this.serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = cache;
    this.codec = NamingCodecFactory.createLookupCodec(factory);
    this.replyQueue = new LinkedBlockingQueue<>();

    this.transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), 0,
        new SyncStage<>(new NamingLookupClientHandler(
            new NamingLookupResponseHandler(this.replyQueue), this.codec)),
        null, retryCount, retryTimeout);

    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }

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
   * Finds an address for an identifier
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
        int retryCount = origRetryCount;
        while(true){
          try {
            return remoteLookup(id);
          } catch (final NamingException e) {
            if(retryCount<=0) {
              throw e;
            } else{
              final int retryTimeout = NameLookupClient.this.retryTimeout
                  * (origRetryCount - retryCount + 1);
              LOG.log(Level.WARNING,
                  "Caught Naming Exception while looking up " + id
                      + " with Name Server. Will retry " + retryCount
                      + " time(s) after waiting for " + retryTimeout + " msec.");
              Thread.sleep(retryTimeout * retryCount);
              --retryCount;
            }
          }
        }
      }

    });
  }

  /**
   * Retrieves an address for an identifier remotely
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

      LOG.log(Level.INFO, "Looking up {0} on NameServer {1}", new Object[] { id, serverSocketAddr });

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
   * Closes resources
   */
  @Override
  public void close() throws Exception {
    // Should not close transport as we did not
    // create it
  }
}

/**
 * Naming lookup client transport event handler
 */
class NamingLookupClientHandler implements EventHandler<TransportEvent> {

  private final EventHandler<NamingLookupResponse> handler;
  private final Codec<NamingMessage> codec;

  NamingLookupClientHandler(final EventHandler<NamingLookupResponse> handler, final Codec<NamingMessage> codec)  {
    this.handler = handler;
    this.codec = codec;
  }

  @Override
  public void onNext(final TransportEvent value) {
    handler.onNext((NamingLookupResponse)codec.decode(value.getData()));
  }

}

/**
 * Naming lookup response handler
 */
class NamingLookupResponseHandler implements EventHandler<NamingLookupResponse> {

  private final BlockingQueue<NamingLookupResponse> replyQueue;

  NamingLookupResponseHandler(final BlockingQueue<NamingLookupResponse> replyQueue) {
    this.replyQueue = replyQueue;
  }

  @Override
  public void onNext(final NamingLookupResponse value) {
    replyQueue.offer(value);
  }
}
