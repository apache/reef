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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.codec.http.*;
import org.apache.reef.wake.remote.transport.TransportFactory.ProtocolType;


/**
 * Netty channel initializer for Transport.
 */
class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

  /**
   * the buffer size of the frame decoder.
   */
  public static final int MAXFRAMELENGTH = 10 * 1024 * 1024;
  private static final int MAX_HTTP_MESSAGE_LENGTH = 10 * 1024 * 1024;
  private final NettyChannelHandlerFactory handlerFactory;

  /**
   * Type of protocol channel use.
   */
  private final ProtocolType protocolType;
  private final boolean isServer;

  NettyChannelInitializer(
      final NettyChannelHandlerFactory handlerFactory,
      final ProtocolType protocol) {
    this(handlerFactory, protocol, false);
  }

  NettyChannelInitializer(
      final NettyChannelHandlerFactory handlerFactory,
      final ProtocolType protocol,
      final boolean isServer) {
    this.handlerFactory = handlerFactory;
    this.protocolType = protocol;
    this.isServer = isServer;
  }

  @Override
  protected void initChannel(final SocketChannel ch) throws Exception {
    final ChannelPipeline pipeline = ch.pipeline();
    switch (this.protocolType) {
    case TCP:
      pipeline
          .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAXFRAMELENGTH, 0, 4, 0, 4))
          .addLast("bytesDecoder", new ByteArrayDecoder())
          .addLast("frameEncoder", new LengthFieldPrepender(4))
          .addLast("bytesEncoder", new ByteArrayEncoder())
          .addLast("chunker", new ChunkedReadWriteHandler());
      break;
    case HTTP:
      pipeline
          .addLast("codec", isServer ? new HttpServerCodec() : new HttpClientCodec())
          .addLast("aggregator", new HttpObjectAggregator(MAX_HTTP_MESSAGE_LENGTH));
      break;
    default:
      throw new IllegalArgumentException("Invalid type of channel");
    }
    // every channel pipeline has the same inbound handler.
    pipeline.addLast("handler", handlerFactory.createChannelInboundHandler());
  }
}
