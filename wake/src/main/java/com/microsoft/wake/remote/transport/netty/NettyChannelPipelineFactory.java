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

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;


/**
 * Netty channel pipeline factory implementation for Transport
 *
 * MAXFRAMELENGTH : the buffer size of the frame decoder
 */
class NettyChannelPipelineFactory implements ChannelPipelineFactory {

  public static final int MAXFRAMELENGTH = 10 * 1024 * 1024;
  private final String tag;
  private final ChannelGroup channelGroup;
  private final NettyChannelHandlerFactory handlerFactory;
  private final NettyEventListener listener;

  /**
   * Constructs a channel pipeline factory
   *
   * @param tag the name of the pipeline handler
   * @param channelGroup the channel group
   * @param listener the Netty event listener
   * @param handlerFactory the channel handler factory
   */
  public NettyChannelPipelineFactory(final String tag,
      final ChannelGroup channelGroup,
      final NettyEventListener listener,
      final NettyChannelHandlerFactory handlerFactory) {
    this.tag = tag;
    this.channelGroup = channelGroup;
    this.handlerFactory = handlerFactory;
    this.listener = listener;
  }

  /**
   * Create a channel pipeline based on byte array encoder/decoder
   * and length field frame encoder/decoder.
   *
   * @return a channel pipeline.
   * @throws Exception
   */
  @Override
  public ChannelPipeline getPipeline() throws Exception {

    final ChannelPipeline pipe = Channels.pipeline();

    pipe.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAXFRAMELENGTH, 0, 4, 0, 4));
    pipe.addLast("protobufDecoder", new ByteArrayDecoder());
    pipe.addLast("frameEncoder", new LengthFieldPrepender(4));
    pipe.addLast("protobufEncoder", new ByteArrayEncoder());
    pipe.addLast("chunker", new ChunkedReadWriteHandler());
    pipe.addLast("handler", handlerFactory.createUpstreamHandler(tag, channelGroup, listener));

    return pipe;
  }
}



