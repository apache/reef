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
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
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

  private final URI uri;
  private final NettyLinkFactory linkFactory;

  NettyHttpServerEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage,
      final URI uri) {
    super(addrToLinkRefMap, stage);
    this.uri = uri;
    this.linkFactory = new NettyHttpLinkFactory<>(uri);
  }


  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    final Channel channel = ctx.channel();

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Channel active. key: {0}", channel.remoteAddress());
    }

    this.addrToLinkRefMap.putIfAbsent(
        channel.remoteAddress(), new LinkReference(linkFactory.newInstance(
            channel, new ByteCodec(), new LoggingLinkListener<byte[]>())));

    LOG.log(Level.FINER, "Add connected channel ref: {0}", this.addrToLinkRefMap.get(channel.remoteAddress()));

  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if(msg instanceof FullHttpRequest) {
      final FullHttpRequest request = (FullHttpRequest) msg;
      final HttpHeaders headers = request.headers();
      final ByteBuf byteBuf = request.content();
      final Channel channel = ctx.channel();
      final byte[] content;

      if (byteBuf.hasArray()) {
        content = byteBuf.array();
      } else {
        content = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(content);
      }

      if (LOG.isLoggable(Level.FINEST)) {
        // log header to trailing header contents.
        final StringBuilder buf = new StringBuilder();
        if (!headers.isEmpty()) {
          for (final Map.Entry<String, String> h : headers) {
            final CharSequence key = h.getKey();
            final CharSequence value = h.getValue();
            buf.append("HEADER: ").append(key).append(" = ").append(value).append("\r\n");
          }
          buf.append("\r\n");
        }
        appendDecoderResult(buf, request);
        if (byteBuf.isReadable()) {
          buf.append("CONTENT: ");
          buf.append(byteBuf.toString(CharsetUtil.UTF_8));
          buf.append("\r\n");
          appendDecoderResult(buf, request);
        }

        buf.append("END OF CONTENT\r\n");
        if (!request.trailingHeaders().isEmpty()) {
          buf.append("\r\n");
          for (CharSequence name : request.trailingHeaders().names()) {
            for (CharSequence value : request.trailingHeaders().getAll(name)) {
              buf.append("TRAILING HEADER: ");
              buf.append(name).append(" = ").append(value).append("\r\n");
            }
          }
          buf.append("\r\n");
        }
        LOG.log(Level.FINEST, "Received Message:\n{0}", buf.toString());
        LOG.log(Level.FINEST, "MessageEvent: local: {0} remote: {1} :: {2}", new Object[]{
                channel.localAddress(), channel.remoteAddress(), byteBuf});
      }

      if (content.length > 0) {
        // send to the dispatch stage
        this.stage.onNext(this.getTransportEvent(content, channel));
      }
    } else {
      LOG.log(Level.SEVERE, "Unknown type of message received: {0}", msg);
      throw new RemoteRuntimeException("Unknown type of message received " + msg);
    }
  }

  private static void appendDecoderResult(final StringBuilder buf, final HttpObject o) {
    final DecoderResult result = o.getDecoderResult();
    if (!result.isSuccess()) {
      buf.append(".. WITH DECODER FAILURE: ").append(result.cause()).append("\r\n");
    }
  }



  @Override
  protected TransportEvent getTransportEvent(final byte[] message, final Channel channel) {
    return new TransportEvent(message, linkFactory.newInstance(channel, new ByteEncoder()));
  }

  @Override
  protected void exceptionCleanup(final ChannelHandlerContext ctx, final Throwable cause) {
    // noop
  }
}
