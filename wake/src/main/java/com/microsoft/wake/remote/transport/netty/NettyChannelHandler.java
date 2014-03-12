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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;

class NettyChannelHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = Logger.getLogger(NettyChannelHandler.class.getName());

  private final String tag;
  private final ChannelGroup channelGroup;
  private final NettyEventListener listener;

  /**
   * Constructs a Netty channel handler
   * 
   * @param channelGroup the channel group
   * @param listener the Netty event listener
   */
  public NettyChannelHandler(
      final String tag, final ChannelGroup channelGroup, final NettyEventListener listener) {
    this.tag = tag;
    this.channelGroup = channelGroup;
    this.listener = listener;
  }
  
  /**
   * Handles the specified upstream event
   * 
   * @param ctx the context object for this handler
   * @param event the upstream event to process or intercept
   * @throws Exception
   */
  @Override
  public void handleUpstream(
      final ChannelHandlerContext ctx, final ChannelEvent event) throws Exception {
    if (event instanceof ChannelStateEvent && LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "{0} {1} local: {2} remote: {3}", new Object[] { this.tag, event,
          event.getChannel().getLocalAddress(), event.getChannel().getRemoteAddress() });
    }
    super.handleUpstream(ctx, event);
  }
  
  /**
   * Handle the incoming message: pass it to the listener.
   * 
   * @param ctx the context object for this handler.
   * @param event the message event.
   * @throws Exception
   */
  @Override
  public void messageReceived(
      final ChannelHandlerContext ctx, final MessageEvent event) throws Exception {
    this.listener.messageReceived(event);
  }
  
  /**
   * Handles the channel connected event
   * 
   * @param ctx the context object for this handler
   * @param event the channel state event
   * @throws Exception
   */
  @Override
  public void channelConnected(
      final ChannelHandlerContext ctx, final ChannelStateEvent event) throws Exception {
    channelGroup.add(event.getChannel());
    listener.channelConnected(event);
    super.channelConnected(ctx, event);
  }
  
  /**
   * Handles the channel closed event
   * 
   * @param ctx the context object for this handler
   * @param event the channel state event
   * @throws Exception
   */
  @Override 
  public void channelClosed(
      final ChannelHandlerContext ctx, final ChannelStateEvent event) throws Exception {
    listener.channelClosed(event);
    super.channelClosed(ctx, event);
  }
  
  /**
   * Handles the exception event
   * 
   * @param ctx the context object for this handler
   * @param event the exception event
   * @throws Exception
   */
  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent event) {
    LOG.log(Level.INFO, "Unexpected exception from downstream. channel: {0} local: {1} remote: {2}",
        new Object[]{event.getChannel(), event.getChannel().getLocalAddress(),
            event.getChannel().getRemoteAddress()});
    LOG.log(Level.WARNING, "Unexpected exception from downstream.", event.getCause());
    event.getChannel().close();
    this.listener.exceptionCaught(event);
  }
}
