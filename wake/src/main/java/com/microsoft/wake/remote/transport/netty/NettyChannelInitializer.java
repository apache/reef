/**
 * Copyright (C) 2014 Microsoft Corporation
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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * Netty channel initializer for Transport
 *
 * MAXFRAMELENGTH : the buffer size of the frame decoder
 */
class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

  public static final int MAXFRAMELENGTH = 10 * 1024 * 1024;
  private final NettyChannelHandlerFactory handlerFactory;

  public NettyChannelInitializer(final NettyChannelHandlerFactory handlerFactory) {
    this.handlerFactory = handlerFactory;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline()
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAXFRAMELENGTH, 0, 4, 0, 4))
        .addLast("bytesDecoder", new ByteArrayDecoder())
        .addLast("frameEncoder", new LengthFieldPrepender(4))
        .addLast("bytesEncoder", new ByteArrayEncoder())
        .addLast("chunker", new ChunkedReadWriteHandler())
        .addLast("handler", handlerFactory.createChannelInboundHandler());
  }
}
