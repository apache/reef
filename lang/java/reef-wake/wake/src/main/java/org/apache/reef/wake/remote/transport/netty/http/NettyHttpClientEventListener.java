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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.netty.AbstractNettyEventListener;
import org.apache.reef.wake.remote.transport.netty.LinkReference;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Netty event listener for server.
 */
final class NettyHttpClientEventListener extends AbstractNettyEventListener {

  private static final Logger LOG = Logger.getLogger(NettyHttpClientEventListener.class.getName());
  private StringBuilder buf = new StringBuilder();

  NettyHttpClientEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage) {
    super(addrToLinkRefMap, stage);
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof HttpResponse) {
      System.out.println("CLIENT : IT IS HTTP RESPONSE");
    } else if (msg instanceof HttpContent) {
      System.out.println("CLIENT : It is HttpContent");
      HttpContent httpContent = (HttpContent) msg;
      ByteBuf content = httpContent.content();
      if (content.isReadable()) {
        buf.append("CONTENT: ");
        buf.append(content.toString(CharsetUtil.UTF_8));
        buf.append("\r\n");
      }

      if (msg instanceof LastHttpContent) {
        final Channel channel = ctx.channel();
        byte[] message = new byte[content.readableBytes()];
        content.readBytes(message);
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "MessageEvent: local: {0} remote: {1} :: {2}", new Object[]{
                  channel.localAddress(), channel.remoteAddress(), buf});
        }

        if (message.length > 0) {
          // send to the dispatch stage
          this.stage.onNext(this.getTransportEvent(message, channel));
        }
      }
    } else {
      LOG.log(Level.SEVERE, "Unknown type of message received: {0}", msg);
    }

  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    // noop
    LOG.log(Level.FINEST, "{0}", ctx);
  }

  @Override
  protected TransportEvent getTransportEvent(final byte[] message, final Channel channel) {
    return new TransportEvent(message, channel.localAddress(), channel.remoteAddress());
  }

  @Override
  protected void exceptionCleanup(final ChannelHandlerContext ctx, final Throwable cause) {
    this.closeChannel(ctx.channel());
  }
}
