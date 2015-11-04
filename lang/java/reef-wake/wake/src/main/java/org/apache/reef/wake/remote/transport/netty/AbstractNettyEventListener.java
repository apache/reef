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
package org.apache.reef.wake.remote.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic functionality for the Netty event listener.
 * This is a base class for client and server versions.
 */
abstract class AbstractNettyEventListener implements NettyEventListener {

  protected static final Logger LOG = Logger.getLogger(AbstractNettyEventListener.class.getName());

  protected final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap;
  protected final EStage<TransportEvent> stage;
  protected EventHandler<Exception> exceptionHandler;

  AbstractNettyEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage) {
    this.addrToLinkRefMap = addrToLinkRefMap;
    this.stage = stage;
  }

  public void registerErrorHandler(final EventHandler<Exception> handler) {
    LOG.log(Level.FINE, "Set error handler {0}", handler);
    this.exceptionHandler = handler;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    final Channel channel = ctx.channel();
    final byte[] message = (byte[]) msg;

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "MessageEvent: local: {0} remote: {1} :: {2}", new Object[]{
          channel.localAddress(), channel.remoteAddress(), message});
    }

    if (message.length > 0) {
      // send to the dispatch stage
      this.stage.onNext(this.getTransportEvent(message, channel));
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    final Channel channel = ctx.channel();
    LOG.log(Level.WARNING, "ExceptionEvent: local: {0} remote: {1} :: {2}", new Object[]{
        channel.localAddress(), channel.remoteAddress(), cause});
    this.exceptionCleanup(ctx, cause);
    if (this.exceptionHandler != null) {
      this.exceptionHandler.onNext(cause instanceof Exception ? (Exception) cause : new Exception(cause));
    }
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    this.closeChannel(ctx.channel());
  }

  protected abstract TransportEvent getTransportEvent(final byte[] message, final Channel channel);

  protected abstract void exceptionCleanup(final ChannelHandlerContext ctx, Throwable cause);

  protected void closeChannel(final Channel channel) {
    final LinkReference refRemoved =
        channel != null && channel.remoteAddress() != null ?
            this.addrToLinkRefMap.remove(channel.remoteAddress()) : null;
    LOG.log(Level.FINER, "Channel closed: {0}. Link ref found and removed: {1}",
        new Object[]{channel, refRemoved != null});
  }
}
