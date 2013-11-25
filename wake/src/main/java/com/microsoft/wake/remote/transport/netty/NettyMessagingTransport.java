/**
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.microsoft.wake.remote.impl.ByteCodec;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.impl.Tuple2;
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
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Messaging transport implementation with Netty
 */
public class NettyMessagingTransport implements Transport {

  private static final Logger LOG = Logger.getLogger(NettyMessagingTransport.class.getName());
  private final long shutdownTimeout = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private final ClientBootstrap clientBootstrap;
  private final ServerBootstrap serverBootstrap;

  private final ChannelGroup clientChannelGroup;
  private final ChannelGroup serverChannelGroup;

  private final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap;

  private int serverPort;
  private final SocketAddress localAddress;

  private final NettyClientEventListener clientEventListener;
  private final NettyServerEventListener serverEventListener;
  
  /**
   * Constructs a messaging transport
   *
   * @param hostAddress the server host address
   * @param serverPort  the server listening port; when it is 0, randomly assign a port number
   * @param clientStage the client-side stage that handles transport events
   * @param serverStage the server-side stage that handles transport events
   */
  public NettyMessagingTransport(String hostAddress, int serverPort, EStage<TransportEvent> clientStage, EStage<TransportEvent> serverStage) {

    addrToLinkRefMap = new ConcurrentHashMap<SocketAddress, LinkReference>();

    clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new DefaultThreadFactory(NettyMessagingTransport.class.getName())),
        Executors.newCachedThreadPool(new DefaultThreadFactory(NettyMessagingTransport.class.getName()))));
    clientBootstrap.setOption("reuseAddress", true);
    clientBootstrap.setOption("tcpNoDelay", true);
    clientBootstrap.setOption("keepAlive", true);
    clientChannelGroup = new DefaultChannelGroup();
    clientEventListener = new NettyClientEventListener(addrToLinkRefMap, clientStage);

    clientBootstrap.setPipelineFactory(new NettyChannelPipelineFactory("client", clientChannelGroup,
        clientEventListener, new NettyDefaultChannelHandlerFactory()));
  
    serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(new DefaultThreadFactory(NettyMessagingTransport.class.getName())),
        Executors.newCachedThreadPool(new DefaultThreadFactory(NettyMessagingTransport.class.getName()))));
    serverBootstrap.setOption("reuseAddress", true);
    serverBootstrap.setOption("tcpNoDelay", true);
    serverBootstrap.setOption("keepAlive", true);
    serverChannelGroup = new DefaultChannelGroup();
    serverEventListener = new NettyServerEventListener(addrToLinkRefMap, serverStage);

    serverBootstrap.setPipelineFactory(new NettyChannelPipelineFactory("server", serverChannelGroup,
        serverEventListener, new NettyDefaultChannelHandlerFactory()));

    // check serverPort is 0?
    Channel acceptor = null;
    this.serverPort = serverPort;
    if (serverPort < 0) {
      throw new RemoteRuntimeException("Port " + serverPort + " is less than 0.");
    } else if (serverPort == 0) {
      // assign a random port
      Random r = new Random();
      while (this.serverPort == 0) {
        int port = r.nextInt(10000) + 10000;
        try {
          LOG.log(Level.FINEST, "port bind {0}", port);
          acceptor = serverBootstrap.bind(
              new InetSocketAddress(hostAddress, port));
          this.serverPort = port;
        } catch (ChannelException e) {
          LOG.log(Level.FINEST, "port collision", e);
        }
      }
    } else {
      acceptor = serverBootstrap.bind(
          new InetSocketAddress(hostAddress, serverPort));
    }

    if (acceptor.isBound()) {
      localAddress = new InetSocketAddress(hostAddress, this.serverPort);
      serverChannelGroup.add(acceptor);
    } else {
      localAddress = null;
      clientBootstrap.releaseExternalResources();
      serverBootstrap.releaseExternalResources();
      throw new TransportRuntimeException("Cannot bind to " + this.serverPort);
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

    clientChannelGroup.close().awaitUninterruptibly();
    serverChannelGroup.close().awaitUninterruptibly();

    clientBootstrap.releaseExternalResources();
    serverBootstrap.releaseExternalResources();

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
  public <T> Link<T> open(final SocketAddress remoteAddr, Encoder<? super T> encoder, LinkListener<? super T> listener)
      throws IOException {
    LinkReference linkRef = addrToLinkRefMap.get(remoteAddr);
    Link<?> link;
    if (linkRef != null) {
      LOG.log(Level.FINE, "link ref found");
      link = linkRef.getLink();
      if (link != null) {
        if (LOG.isLoggable(Level.FINE)) 
          LOG.log(Level.FINE, "link {0} for {1} found", new Object[] {link, remoteAddr});
        return (Link<T>) link;
      }
    }
    LOG.log(Level.FINE, "No cached link for {0} thread {1}", new Object[]{remoteAddr, Thread.currentThread()});
    // no linkRef
    LinkReference newLinkRef = new LinkReference();
    LinkReference prior = addrToLinkRefMap.putIfAbsent(remoteAddr, newLinkRef);
    AtomicBoolean flag = (prior != null) ? prior.getConnectInProgress() : newLinkRef.getConnectInProgress();
    synchronized (flag) {
      if (!flag.compareAndSet(false, true)) {
        while (flag.get()) {
          try {
            flag.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    linkRef = addrToLinkRefMap.get(remoteAddr);
    link = linkRef.getLink();
    if (link != null)
      return (Link<T>) link;
    else {
      ChannelFuture f = clientBootstrap.connect(remoteAddr);
      f.awaitUninterruptibly();
      Channel channel = f.getChannel();
      link = new NettyLink<T>(channel, encoder, listener);
      linkRef.setLink(link);
    }

    synchronized (flag) {
      flag.compareAndSet(true, false);
      flag.notifyAll();
    }
    return (Link<T>) link;
  }

  /**
   * Returns a link for the remote address if already cached; otherwise, returns null
   *
   * @param remoteAddr the remote address
   * @return a link if already cached; otherwise, null
   */
  public <T> Link<T> get(SocketAddress remoteAddr) {
    LinkReference linkRef = addrToLinkRefMap.get(remoteAddr);
    if (linkRef != null)
      return (Link<T>) linkRef.getLink();
    return null;
  }

  /**
   * Gets a server local socket address of this transport
   *
   * @return a server local socket address
   */
  @Override
  public SocketAddress getLocalAddress() {
    return localAddress;
  }

  /**
   * Gets a server listening port of this transport
   *
   * @return a listening port number
   */
  @Override
  public int getListeningPort() {
    return serverPort;
  }

  /**
   * Registers the exception event handler
   *
   * @param handler the exception event handler
   */
  @Override
  public void registerErrorHandler(EventHandler<Exception> handler) {
    clientEventListener.registerErrorHandler(handler);
    serverEventListener.registerErrorHandler(handler);
  }
}

class LinkReference {

  private Link<?> link;
  private AtomicBoolean connectInProgress = new AtomicBoolean(false);

  LinkReference() {
  }

  LinkReference(Link<?> link) {
    this.link = link;
  }

  synchronized void setLink(Link<?> link) {
    this.link = link;
  }

  synchronized Link<?> getLink() {
    return link;
  }

  AtomicBoolean getConnectInProgress() {
    return connectInProgress;
  }

}

class AddressPair extends Tuple2<SocketAddress, SocketAddress> {

  AddressPair(SocketAddress addr1, SocketAddress addr2) {
    super(addr1, addr2);
  }

}

class NettyClientEventListener implements NettyEventListener {

  private static final Logger LOG = Logger.getLogger(NettyClientEventListener.class.getName());

  private final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap;
  private final EStage<TransportEvent> stage;
  private EventHandler<Exception> handler;

  public NettyClientEventListener(ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
                                  EStage<TransportEvent> stage) {
    this.addrToLinkRefMap = addrToLinkRefMap;
    this.stage = stage;
  }

  public void registerErrorHandler(EventHandler<Exception> handler) {
    LOG.log(Level.FINE, "set error handler {0}", handler);
    this.handler = handler;
  }

  @Override
  public void messageReceived(MessageEvent e) {
    if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "local: " + e.getChannel().getLocalAddress() + " remote: " + e.getChannel().getRemoteAddress() + " " + e.getMessage().toString());

    byte[] message = (byte[]) e.getMessage();
    if (message.length <= 0)
      return;

    // send to the dispatch stage
    stage.onNext(new TransportEvent(message, e.getChannel().getLocalAddress(), e.getChannel().getRemoteAddress()));
  }

  @Override
  public void exceptionCaught(final ExceptionEvent e) {
    final Throwable cause = e.getCause();
    LOG.log(Level.WARNING, "ExceptionEvent: " + e, cause);
    SocketAddress addr = e.getChannel().getRemoteAddress();
    if (addr != null) {
      addrToLinkRefMap.remove(addr);
    }
    if (handler != null) {
      handler.onNext(cause instanceof Exception ? (Exception) cause : new Exception(cause));
    }
  }

  @Override
  public void channelConnected(ChannelStateEvent e) {
  }

  @Override
  public void channelClosed(ChannelStateEvent e) {
    LOG.log(Level.FINE, "Channel Closed. Trying to remove link ref");
    SocketAddress addr = e.getChannel().getRemoteAddress();
    if (addr != null) {
      if (LOG.isLoggable(Level.FINER)) LOG.log(Level.FINER, "key: " + e.getChannel().getRemoteAddress() + " " + e.toString());
      addrToLinkRefMap.remove(addr);
      LOG.log(Level.FINE, "Link ref found and removed");
    } else
      LOG.log(Level.FINE, "No Link ref found");
  }

}

class NettyServerEventListener implements NettyEventListener {

  private static final Logger LOG = Logger.getLogger(NettyServerEventListener.class.getName());

  private final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap;
  private final EStage<TransportEvent> stage;
  private EventHandler<Exception> handler;

  public NettyServerEventListener(ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
                                  EStage<TransportEvent> stage) {
    this.addrToLinkRefMap = addrToLinkRefMap;
    this.stage = stage;
  }

  public void registerErrorHandler(EventHandler<Exception> handler) {
    LOG.log(Level.FINE, "set error handler {0}", handler);
    this.handler = handler;
  }
  
  @Override
  public void messageReceived(MessageEvent e) {
    if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "local: " + e.getChannel().getLocalAddress() + " remote: " + e.getChannel().getRemoteAddress() + " " + e.getMessage().toString());

    // byte[] message
    byte[] message = (byte[]) e.getMessage();
    if (message.length <= 0)
      return;

    Link<byte[]> link = new NettyLink<>(e.getChannel(), new ByteEncoder());
    // send to the dispatch stage
    stage.onNext(new TransportEvent(message, link));
  }

  @Override
  public void exceptionCaught(ExceptionEvent e) {
    final Throwable cause = e.getCause();
    LOG.log(Level.WARNING, "ExceptionEvent: " + e, cause);
    if (handler != null) {
      LOG.log(Level.WARNING, "handler {0} called", handler);
      handler.onNext(cause instanceof Exception ? (Exception) cause : new Exception(cause));
    } else {
      LOG.log(Level.WARNING, "handler {0} is null", handler);
    }
  }

  @Override
  public void channelConnected(ChannelStateEvent e) {
    if (LOG.isLoggable(Level.FINER)) LOG.log(Level.FINER, "key: " + e.getChannel().getRemoteAddress() + " " + e.toString());
    LinkReference ref = addrToLinkRefMap.putIfAbsent(e.getChannel().getRemoteAddress(),
        new LinkReference(new NettyLink<byte[]>(e.getChannel(), new ByteCodec(), new LoggingLinkListener<byte[]>())));
    LOG.log(Level.FINER, "put: {0}", ref);
  }

  @Override
  public void channelClosed(ChannelStateEvent e) {
    LOG.log(Level.FINE, "Channel Closed. Trying to remove link ref");
    if (e.getChannel() != null) {
      if (LOG.isLoggable(Level.FINER)) LOG.log(Level.FINER, "key: " + e.getChannel().getRemoteAddress() + " " + e.toString());
      addrToLinkRefMap.remove(e.getChannel().getRemoteAddress());
      LOG.log(Level.FINE, "Link ref found and removed");
    } else
      LOG.log(Level.FINE, "No Link ref found");
  }

}
