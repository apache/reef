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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.remote.impl.ByteCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.net.SocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * A Netty event listener for server side.
 */
final class NettyHttpServerEventListener extends AbstractNettyEventListener {

  private HttpRequest httpRequest;
  private final StringBuilder buf = new StringBuilder();
  private final URI uri;

  NettyHttpServerEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage,
      final URI uri) {
    super(addrToLinkRefMap, stage);
    this.uri = uri;
  }


  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    final Channel channel = ctx.channel();

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Channel active. key: {0}", channel.remoteAddress());
    }

    this.addrToLinkRefMap.putIfAbsent(
        channel.remoteAddress(), new LinkReference(new NettyLink<>(
            channel, new ByteCodec(), new LoggingLinkListener<byte[]>(), uri)));

    LOG.log(Level.FINER, "Add connected channel ref: {0}", this.addrToLinkRefMap.get(channel.remoteAddress()));

  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if(msg instanceof HttpRequest) {
      LOG.log(Level.FINEST, "HttpRequest received");

      final HttpRequest request = (HttpRequest) msg;
      final HttpHeaders headers = request.headers();
      this.httpRequest = request;

      if (!headers.isEmpty()) {
        for (final Map.Entry<String, String> h: headers) {
          final CharSequence key = h.getKey();
          final CharSequence value = h.getValue();
          buf.append("HEADER: ").append(key).append(" = ").append(value).append("\r\n");
        }
        buf.append("\r\n");
      }
      appendDecoderResult(buf, request);
    } else if (msg instanceof HttpContent) {
      LOG.log(Level.FINEST, "HttpContent received");
      final HttpContent httpContent = (HttpContent) msg;
      final ByteBuf content = httpContent.content();
      if (content.isReadable()) {
        buf.append("CONTENT: ");
        buf.append(content.toString(CharsetUtil.UTF_8));
        buf.append("\r\n");
        appendDecoderResult(buf, this.httpRequest);
      }

      if (msg instanceof LastHttpContent) {
        buf.append("END OF CONTENT\r\n");
        final LastHttpContent trailer = (LastHttpContent) msg;
        if (!trailer.trailingHeaders().isEmpty()) {
          buf.append("\r\n");
          for (CharSequence name: trailer.trailingHeaders().names()) {
            for (CharSequence value: trailer.trailingHeaders().getAll(name)) {
              buf.append("TRAILING HEADER: ");
              buf.append(name).append(" = ").append(value).append("\r\n");
            }
          }
          buf.append("\r\n");
        }

        final Channel channel = ctx.channel();
        final byte[] message = new byte[content.readableBytes()];
        content.readBytes(message);
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "MessageEvent: local: {0} remote: {1} :: {2}", new Object[]{
                  channel.localAddress(), channel.remoteAddress(), content});
        }

        if (message.length > 0) {
          // send to the dispatch stage
          this.stage.onNext(this.getTransportEvent(message, channel));
        }
      }
    } else {
      LOG.log(Level.SEVERE, "Unknown type of message received: {0}", msg);
    }
    LOG.log(Level.FINEST, buf.toString());
  }

  private static void appendDecoderResult(final StringBuilder buf, final HttpObject o) {
    final DecoderResult result = o.getDecoderResult();
    if (result.isSuccess()) {
      return;
    }

    buf.append(".. WITH DECODER FAILURE: ");
    buf.append(result.cause());
    buf.append("\r\n");
  }



  @Override
  protected TransportEvent getTransportEvent(final byte[] message, final Channel channel) {
    return new TransportEvent(message, new NettyLink<>(channel, new ByteEncoder()));
  }

  @Override
  protected void exceptionCleanup(final ChannelHandlerContext ctx, final Throwable cause) {
    // noop
  }
}
