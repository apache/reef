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
  public NameLookupClient(String serverAddr, int serverPort,
      IdentifierFactory factory, int retryCount, int retryTimeout, 
      Cache<Identifier, InetSocketAddress> cache) {
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
  public NameLookupClient(String serverAddr, int serverPort, long timeout,
      IdentifierFactory factory, int retryCount, int retryTimeout, 
      Cache<Identifier, InetSocketAddress> cache) {
    serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = cache;
    replyQueue = new LinkedBlockingQueue<NamingLookupResponse>();
    codec = NamingCodecFactory.createLookupCodec(factory);
    transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), 0,
        new SyncStage<TransportEvent>(new NamingLookupClientHandler(
            new NamingLookupResponseHandler(replyQueue), codec)), null);
    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }
  
  NameLookupClient(String serverAddr, int serverPort, long timeout,
      IdentifierFactory factory, int retryCount, int retryTimeout,
      BlockingQueue<NamingLookupResponse> replyQueue, Transport transport,
      Cache<Identifier, InetSocketAddress> cache) {
    serverSocketAddr = new InetSocketAddress(serverAddr, serverPort);
    this.timeout = timeout;
    this.cache = cache;
    codec = NamingCodecFactory.createFullCodec(factory);
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
        int retryCount = NameLookupClient.this.retryCount;
        int retryTimeout = NameLookupClient.this.retryTimeout;
        while(true){
          try {
            return remoteLookup(id);
          } catch (NamingException e) {
            if(retryCount<=0)
              throw e;
            else{
              LOG.log(Level.WARNING,
                  "Caught Naming Exception while looking up " + id
                      + " with Name Server. Will retry " + retryCount
                      + " time(s) after waiting for " + retryTimeout + " msec.");
              Thread.sleep(retryTimeout);
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
  public InetSocketAddress remoteLookup(Identifier id) throws Exception {
    // the lookup is not thread-safe, because concurrent replies may
    // be read by the wrong thread.
    // TODO: better fix uses a map of id's after REEF-198
    synchronized (this) {

      LOG.log(Level.INFO, "Looking up " + id.toString() + " on NameServer " + serverSocketAddr.toString());

      List<Identifier> ids = Arrays.asList(id);
      Link<NamingMessage> link = transport.open(serverSocketAddr, codec, 
          new LoggingLinkListener<NamingMessage>());
      link.write(new NamingLookupRequest(ids));

      NamingLookupResponse resp;
      while(true) {
        try {
          resp = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
          break;
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new NamingException(e);
        }
      }

      List<NameAssignment> list = resp.getNameAssignments();
      if (list.size() <= 0)
        throw new NamingException("Cannot find " + id + " from the name server");
      else
        return list.get(0).getAddress();
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
  
  NamingLookupClientHandler(EventHandler<NamingLookupResponse> handler, Codec<NamingMessage> codec)  {
    this.handler = handler;
    this.codec = codec;
  }
  
  @Override
  public void onNext(TransportEvent value) {
    handler.onNext((NamingLookupResponse)codec.decode(value.getData()));    
  }
  
}

/**
 * Naming lookup response handler
 */
class NamingLookupResponseHandler implements EventHandler<NamingLookupResponse> {

  private final BlockingQueue<NamingLookupResponse> replyQueue;

  NamingLookupResponseHandler(BlockingQueue<NamingLookupResponse> replyQueue) {
    this.replyQueue = replyQueue;
  }
  
  @Override
  public void onNext(NamingLookupResponse value) {
    replyQueue.offer(value);
  }
  
}
