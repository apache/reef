/**
 * Copyright (C) 2012 Microsoft Corporation
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
package com.microsoft.wake.remote.transport.netty;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.impl.DefaultThreadFactory;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.LinkListener;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.exception.TransportRuntimeException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Messaging transport implementation with Netty
 */
public class NettyMessagingTransport implements Transport {

  private static final String CLASS_NAME = NettyMessagingTransport.class.getName();
  private static final Logger LOG = Logger.getLogger(CLASS_NAME);

  private static final long SHUTDOWN_TIMEOUT = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap = new ConcurrentHashMap<>();

  private final ClientBootstrap clientBootstrap;
  private final ServerBootstrap serverBootstrap;

  private final ChannelGroup clientChannelGroup = new DefaultChannelGroup();
  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup();

  private final int serverPort;
  private final SocketAddress localAddress;

  private final NettyClientEventListener clientEventListener;
  private final NettyServerEventListener serverEventListener;
  
  private final int numberOfTries;
  private final int retryTimeout;
  
  /**
   * Constructs a messaging transport
   *
   * @param hostAddress the server host address
   * @param port  the server listening port; when it is 0, randomly assign a port number
   * @param clientStage the client-side stage that handles transport events
   * @param serverStage the server-side stage that handles transport events
   * @deprecated in 0.4. Please use the other constructor instead.
   */
  @Deprecated
  public NettyMessagingTransport(final String hostAddress, int port,
                                 final EStage<TransportEvent> clientStage,
                                 final EStage<TransportEvent> serverStage) {

    this(hostAddress, port, clientStage, serverStage, 3, 10000);
  }

  /**
   * Constructs a messaging transport 
   * @param hostAddress the server host address
   * @param port  the server listening port; when it is 0, randomly assign a port number
   * @param clientStage the client-side stage that handles transport events
   * @param serverStage the server-side stage that handles transport events
   * @param numberOfTries the number of tries of reconnection 
   * @param retryTimeout the timeout of reconnection 
   */
  public NettyMessagingTransport(final String hostAddress, int port,
                                 final EStage<TransportEvent> clientStage,
                                 final EStage<TransportEvent> serverStage,
                                 int numberOfTries,
                                 int retryTimeout) {

    if (port < 0) {
      throw new RemoteRuntimeException("Invalid server port: " + port);
    }

    this.numberOfTries = numberOfTries;
    this.retryTimeout = retryTimeout;
    this.clientEventListener = new NettyClientEventListener(this.addrToLinkRefMap, clientStage);
    this.serverEventListener = new NettyServerEventListener(this.addrToLinkRefMap, serverStage);

    this.clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME)),
        Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME))));
    this.clientBootstrap.setOption("reuseAddress", true);
    this.clientBootstrap.setOption("tcpNoDelay", true);
    this.clientBootstrap.setOption("keepAlive", true);
    this.clientBootstrap.setPipelineFactory(new NettyChannelPipelineFactory("client",
        this.clientChannelGroup, this.clientEventListener, new NettyDefaultChannelHandlerFactory()));

    this.serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME)),
        Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME))));

    this.serverBootstrap.setOption("reuseAddress", true);
    this.serverBootstrap.setOption("tcpNoDelay", true);
    this.serverBootstrap.setOption("keepAlive", true);
    this.serverBootstrap.setPipelineFactory(new NettyChannelPipelineFactory("server",
        this.serverChannelGroup, this.serverEventListener, new NettyDefaultChannelHandlerFactory()));

    Channel acceptor = null;
    if (port > 0) {
      acceptor = this.serverBootstrap.bind(new InetSocketAddress(hostAddress, port));
    } else {
      final Random rand = new Random();
      while (acceptor == null) {
        port = rand.nextInt(10000) + 10000;
        try {
          LOG.log(Level.FINEST, "Try port {0}", port);
          acceptor = this.serverBootstrap.bind(new InetSocketAddress(hostAddress, port));
        } catch (final ChannelException ex) {
          LOG.log(Level.WARNING, "Port collision", ex);
        }
      }
    }

    this.serverPort = port;

    if (acceptor.isBound()) {

      this.localAddress = new InetSocketAddress(hostAddress, this.serverPort);
      this.serverChannelGroup.add(acceptor);

    } else {
      final RuntimeException transportException =
          new TransportRuntimeException("Cannot bind to " + this.serverPort);
      LOG.log(Level.SEVERE, "Cannot bind to " + this.serverPort, transportException);
      this.clientBootstrap.releaseExternalResources();
      this.serverBootstrap.releaseExternalResources();
      throw transportException;
    }

    LOG.log(Level.FINE, "Starting netty transport socket address: {0}", this.localAddress);
  }

  /**
   * Closes all channels and releases all resources
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {

    LOG.log(Level.FINE, "Closing netty transport socket address: {0}", this.localAddress);

    this.clientChannelGroup.close().awaitUninterruptibly();
    this.serverChannelGroup.close().awaitUninterruptibly();

    this.clientBootstrap.releaseExternalResources();
    this.serverBootstrap.releaseExternalResources();

    LOG.log(Level.FINE, "Closing netty transport socket address: {0} done", this.localAddress);
  }

  /**
   * Returns a link for the remote address if cached; otherwise opens, caches and returns
   * When it opens a link for the remote address, only one attempt for the address is made at a given time
   *
   * @param remoteAddr the remote socket address
   * @param encoder    the encoder
   * @param listener   the link listener
   * @return a link associated with the address
   * @throws IOException
   */
  @Override
  public <T> Link<T> open(final SocketAddress remoteAddr, final Encoder<? super T> encoder,
                          final LinkListener<? super T> listener) throws IOException {

    Link<T> link = null;

    for(int i = 0; i < this.numberOfTries; i++){

      LinkReference linkRef = this.addrToLinkRefMap.get(remoteAddr);

      if (linkRef != null) {
        link = (Link<T>) linkRef.getLink();
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Link {0} for {1} found", new Object[] {link, remoteAddr});
        }
        if (link != null) {
          return link;
        }
      }

      LOG.log(Level.FINE, "No cached link for {0} thread {1}",
          new Object[]{remoteAddr, Thread.currentThread()});

      // no linkRef
      final LinkReference newLinkRef = new LinkReference();
      final LinkReference prior = this.addrToLinkRefMap.putIfAbsent(remoteAddr, newLinkRef);
      final AtomicBoolean flag = prior != null ?
          prior.getConnectInProgress() : newLinkRef.getConnectInProgress();

      synchronized (flag) {
        if (!flag.compareAndSet(false, true)) {
          while (flag.get()) {
            try {
              flag.wait();
            } catch (final InterruptedException ex) {
              LOG.log(Level.WARNING, "Wait interrupted", ex);
            }
          }
        }
      }

      linkRef = this.addrToLinkRefMap.get(remoteAddr);
      link = (Link<T>) linkRef.getLink();

      if (link != null) {
        return link;
      }

      ChannelFuture connectFuture = null;
      try{
        connectFuture = this.clientBootstrap.connect(remoteAddr);
        connectFuture.syncUninterruptibly();
        link = new NettyLink<>(connectFuture.getChannel(), encoder, listener);
        linkRef.setLink(link);

        synchronized (flag) {
          flag.compareAndSet(true, false);
          flag.notifyAll();
        }
        break;
      }catch(Exception e){
        if(e.getCause().getClass().getSimpleName().compareTo("ConnectException") == 0){
          LOG.log(Level.WARNING, "Connection Refused... Retrying {0} of {1}", new Object[] {i+1, this.numberOfTries});
          synchronized (flag) {
            flag.compareAndSet(true, false);
            flag.notifyAll();
          }

          if(i < this.numberOfTries){
            try {
              Thread.sleep(retryTimeout);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }
        }else{
          throw e;
        }
      }
    }

    return link;
  }

  /**
   * Returns a link for the remote address if already cached; otherwise, returns null
   *
   * @param remoteAddr the remote address
   * @return a link if already cached; otherwise, null
   */
  public <T> Link<T> get(final SocketAddress remoteAddr) {
    final LinkReference linkRef = this.addrToLinkRefMap.get(remoteAddr);
    return linkRef != null ? (Link<T>) linkRef.getLink() : null;
  }

  /**
   * Gets a server local socket address of this transport
   *
   * @return a server local socket address
   */
  @Override
  public SocketAddress getLocalAddress() {
    return this.localAddress;
  }

  /**
   * Gets a server listening port of this transport
   *
   * @return a listening port number
   */
  @Override
  public int getListeningPort() {
    return this.serverPort;
  }

  /**
   * Registers the exception event handler
   *
   * @param handler the exception event handler
   */
  @Override
  public void registerErrorHandler(final EventHandler<Exception> handler) {
    this.clientEventListener.registerErrorHandler(handler);
    this.serverEventListener.registerErrorHandler(handler);
  }
}
