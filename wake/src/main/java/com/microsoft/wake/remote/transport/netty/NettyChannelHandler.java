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
  private final ChannelGroup channelGroup;
  private final NettyEventListener listener;
  private final String tag;
  
  /**
   * Constructs a Netty channel handler
   * 
   * @param channelGroup the channel group
   * @param listener the Netty event listener
   */
  public NettyChannelHandler(String tag, ChannelGroup channelGroup, NettyEventListener listener) {
    this.channelGroup = channelGroup;
    this.listener = listener;
    this.tag = tag;
  }
  
  /**
   * Handles the specified upstream event
   * 
   * @param ctx the context object for this handler
   * @param e the upstream event to process or intercept
   * @throws Exception
   */
  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, tag + " " + e.toString() + " local: " + e.getChannel().getLocalAddress() + 
          " remote: " + e.getChannel().getRemoteAddress());
    }
    super.handleUpstream(ctx, e);
  }
  
  /**
   * Handles the received message
   * 
   * @param ctx the context object for this handler
   * @param e the message event
   * @throws Exception
   */
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    listener.messageReceived(e);
  }
  
  /**
   * Handles the channel connected event
   * 
   * @param ctx the context object for this handler
   * @param e the channel state event
   * @throws Exception
   */
  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    channelGroup.add(e.getChannel());
    listener.channelConnected(e);
    super.channelConnected(ctx, e);
  }
  
  /**
   * Handles the channel closed event
   * 
   * @param ctx the context object for this handler
   * @param e the channel state event
   * @throws Exception
   */
  @Override 
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    listener.channelClosed(e);
    super.channelClosed(ctx, e);
  }
  
  /**
   * Handles the exception event
   * 
   * @param ctx the context object for this handler
   * @param e the exception event
   * @throws Exception
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    LOG.log(Level.WARNING, "Unexpected exception from downstream. " + " channel: " + e.getChannel() + 
        " local: " + e.getChannel().getLocalAddress() + 
        " remote: " + e.getChannel().getRemoteAddress() + 
        " cause: " + e.getCause());
    e.getCause().printStackTrace();
    e.getChannel().close();
    listener.exceptionCaught(e);
  }

}
