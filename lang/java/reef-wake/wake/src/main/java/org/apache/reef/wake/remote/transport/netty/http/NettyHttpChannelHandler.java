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
package org.apache.reef.wake.remote.transport.netty.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.ReferenceCountUtil;
import org.apache.reef.wake.remote.transport.netty.NettyEventListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty channel handler for channel status(active/inactive) and incoming data.
 */
final class NettyHttpChannelHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = Logger.getLogger(NettyHttpChannelHandler.class.getName());

  private final String tag;
  private final ChannelGroup channelGroup;
  private final NettyEventListener listener;

  /**
   * Constructs a Netty channel handler.
   *
   * @param tag          tag string
   * @param channelGroup the channel group
   * @param listener     the Netty event listener
   */
  NettyHttpChannelHandler(
      final String tag, final ChannelGroup channelGroup, final NettyEventListener listener) {
    this.tag = tag;
    this.channelGroup = channelGroup;
    this.listener = listener;
  }

  /**
   * Handle the incoming message: pass it to the listener.
   *
   * @param ctx the context object for this handler.
   * @param msg the message.
   * @throws Exception
   */
  @Override
  public void channelRead(
      final ChannelHandlerContext ctx, final Object msg) throws Exception {
    try {
      this.listener.channelRead(ctx, msg);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * Flushes after channel read complete.
   *
   * @param ctx
   * @throws Exception
   */

  @Override
  public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  /**
   * Handles the channel active event.
   *
   * @param ctx the context object for this handler
   * @throws Exception
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    this.channelGroup.add(ctx.channel());
    this.listener.channelActive(ctx);
    super.channelActive(ctx);
  }

  /**
   * Handles the channel inactive event.
   *
   * @param ctx the context object for this handler
   * @throws Exception
   */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    this.listener.channelInactive(ctx);
    super.channelInactive(ctx);
  }

  /**
   * Handles the exception event.
   *
   * @param ctx   the context object for this handler
   * @param cause the cause
   * @throws Exception
   */
  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    final Channel channel = ctx.channel();

    LOG.log(Level.INFO, "Unexpected exception from downstream. channel: {0} local: {1} remote: {2}",
        new Object[]{channel, channel.localAddress(), channel.remoteAddress()});
    LOG.log(Level.WARNING, "Unexpected exception from downstream.", cause);
    channel.close();
    this.listener.exceptionCaught(ctx, cause);
  }

}
