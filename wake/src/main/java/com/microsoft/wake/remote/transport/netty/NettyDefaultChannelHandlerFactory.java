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

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

/**
 * Default Netty channel handler factory
 */
final class NettyDefaultChannelHandlerFactory implements NettyChannelHandlerFactory {

  private final String tag;
  private final ChannelGroup group;
  private final NettyEventListener listener;

  NettyDefaultChannelHandlerFactory(
      final String tag, final ChannelGroup group, final NettyEventListener listener) {
    this.tag = tag;
    this.group = group;
    this.listener = listener;
  }

  /**
   * Creates a Netty channel handler
   * @return a simple channel upstream handler.
   */
  @Override
  public ChannelInboundHandlerAdapter createChannelInboundHandler() {
    return new NettyChannelHandler(this.tag, this.group, this.listener);
  }
}
