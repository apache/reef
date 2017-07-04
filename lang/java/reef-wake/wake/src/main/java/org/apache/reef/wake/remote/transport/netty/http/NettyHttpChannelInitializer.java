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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import org.apache.reef.wake.remote.transport.netty.NettyChannelHandlerFactory;

/**
 * Netty Http channel initializer for Transport.
 */
class NettyHttpChannelInitializer extends ChannelInitializer<SocketChannel> {

  static final int TYPE_SERVER = 0;
  static final int TYPE_CLIENT = 1;

  private final NettyChannelHandlerFactory handlerFactory;
  private final SslContext sslContext;
  private final int type;

  NettyHttpChannelInitializer(
          final NettyChannelHandlerFactory handlerFactory,
          final SslContext sslContext,
          final int type) {
    if(type != TYPE_SERVER && type != TYPE_CLIENT){
      throw new IllegalArgumentException("Invalid type of channel");
    }
    this.handlerFactory = handlerFactory;
    this.sslContext = sslContext;
    this.type = type;
  }

  @Override
  protected void initChannel(final SocketChannel ch) throws Exception {
    if(sslContext != null) {
      ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
    }
    if(type == TYPE_SERVER) {
      // Init channel for server
      ch.pipeline()
          .addLast("codec", new HttpServerCodec())
          .addLast("requestDecoder", new HttpRequestDecoder())
          .addLast("responseEncoder", new HttpResponseEncoder());
    } else if (type == TYPE_CLIENT) {
      // Init channel for client
      ch.pipeline()
          .addLast("codec", new HttpClientCodec())
          .addLast("decompressor", new HttpContentDecompressor());
    }
    ch.pipeline().addLast("handler", handlerFactory.createChannelInboundHandler());
  }
}
