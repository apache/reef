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
import com.microsoft.wake.impl.DefaultThreadFactory;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.LinkListener;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.exception.TransportRuntimeException;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Messaging transport implementation with Netty
 */
public class NettyMessagingTransport implements Transport {

  private static final String CLASS_NAME = NettyMessagingTransport.class.getName();
  private static final Logger LOG = Logger.getLogger(CLASS_NAME);

  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 20;
  private static final int CLIENT_WORKER_NUM_THREADS = 10;
  
  private final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap = new ConcurrentHashMap<>();

  private final EventLoopGroup clientWorkerGroup;
  private final EventLoopGroup serverBossGroup;
  private final EventLoopGroup serverWorkerGroup;
  
  private final Bootstrap clientBootstrap;
  private final ServerBootstrap serverBootstrap;
  private final Channel acceptor;

  private final ChannelGroup clientChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private final int serverPort;
  private final SocketAddress localAddress;

  private final NettyClientEventListener clientEventListener;
  private final NettyServerEventListener serverEventListener;
  
  /**
   * Constructs a messaging transport
   *
   * @param hostAddress the server host address
   * @param port  the server listening port; when it is 0, randomly assign a port number
   * @param clientStage the client-side stage that handles transport events
   * @param serverStage the server-side stage that handles transport events
   */
  public NettyMessagingTransport(final String hostAddress, int port,
                                 final EStage<TransportEvent> clientStage,
                                 final EStage<TransportEvent> serverStage) {

    if (port < 0) {
      throw new RemoteRuntimeException("Invalid server port: " + port);
    }

    this.clientEventListener = new NettyClientEventListener(this.addrToLinkRefMap, clientStage);
    this.serverEventListener = new NettyServerEventListener(this.addrToLinkRefMap, serverStage);

    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS, new DefaultThreadFactory(CLASS_NAME + "ServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS, new DefaultThreadFactory(CLASS_NAME + "ServerWorker"));
    this.clientWorkerGroup = new NioEventLoopGroup(CLIENT_WORKER_NUM_THREADS, new DefaultThreadFactory(CLASS_NAME + "ClientWorker"));
    
    this.clientBootstrap = new Bootstrap();
    this.clientBootstrap.group(this.clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyDefaultChannelHandlerFactory("client",
        this.clientChannelGroup, this.clientEventListener)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
    
    this.serverBootstrap = new ServerBootstrap();
    this.serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(new NettyDefaultChannelHandlerFactory("server",
          this.serverChannelGroup, this.serverEventListener)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);    

    LOG.log(Level.FINE, "Binding to {0}", port);
    
    Channel acceptor = null;
    try {
      if (port > 0) {
        acceptor = this.serverBootstrap.bind(new InetSocketAddress(hostAddress, port)).sync().channel();
      } else {
        final Random rand = new Random();
        while (acceptor == null) {
          port = rand.nextInt(10000) + 10000;
          LOG.log(Level.FINEST, "Try port {0}", port);
          acceptor = this.serverBootstrap.bind(new InetSocketAddress(hostAddress, port)).sync().channel();
        }
      }
    } catch (Exception ex) {
      final RuntimeException transportException =
          new TransportRuntimeException("Cannot bind to " + this.serverPort);
      LOG.log(Level.SEVERE, "Cannot bind to " + this.serverPort, transportException);
      this.clientWorkerGroup.shutdownGracefully();
      this.serverBossGroup.shutdownGracefully();
      this.serverWorkerGroup.shutdownGracefully();
      throw transportException;
    }
    
    this.acceptor = acceptor;
    this.serverPort = port;
    this.localAddress = new InetSocketAddress(hostAddress, this.serverPort);

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

    this.acceptor.close().sync();
    this.clientWorkerGroup.shutdownGracefully();
    this.serverBossGroup.shutdownGracefully();
    this.serverWorkerGroup.shutdownGracefully();
    
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

    LinkReference linkRef = this.addrToLinkRefMap.get(remoteAddr);
    Link<T> link;

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

    final ChannelFuture connectFuture = this.clientBootstrap.connect(remoteAddr);
    connectFuture.awaitUninterruptibly();

    link = new NettyLink<>(connectFuture.channel(), encoder, listener);
    linkRef.setLink(link);

    synchronized (flag) {
      flag.compareAndSet(true, false);
      flag.notifyAll();
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
