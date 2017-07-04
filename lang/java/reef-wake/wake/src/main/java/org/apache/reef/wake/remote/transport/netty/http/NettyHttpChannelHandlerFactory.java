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

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.apache.reef.wake.remote.transport.netty.NettyChannelHandlerFactory;
import org.apache.reef.wake.remote.transport.netty.NettyEventListener;

/**
 * Default Netty channel handler factory.
 */
final class NettyHttpChannelHandlerFactory implements NettyChannelHandlerFactory {

  private final String tag;
  private final ChannelGroup group;
  private final NettyEventListener listener;

  NettyHttpChannelHandlerFactory(
      final String tag, final ChannelGroup group, final NettyEventListener listener) {
    this.tag = tag;
    this.group = group;
    this.listener = listener;
  }

  /**
   * Creates a Netty channel handler.
   *
   * @return a simple channel upstream handler.
   */
  @Override
  public ChannelInboundHandlerAdapter createChannelInboundHandler() {
    return new NettyHttpChannelHandler(this.tag, this.group, this.listener);
  }
}
