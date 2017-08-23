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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.remote.impl.ByteCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * A Netty event listener for server side.
 */
final class NettyServerEventListener extends AbstractNettyEventListener {

  private NettyLinkFactory linkFactory = new NettyDefaultLinkFactory();

  NettyServerEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage) {
    super(addrToLinkRefMap, stage);
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
  protected TransportEvent getTransportEvent(final byte[] message, final Channel channel) {
    return new TransportEvent(message, linkFactory.newInstance(channel, new ByteEncoder()));
  }

  @Override
  protected void exceptionCleanup(final ChannelHandlerContext ctx, final Throwable cause) {
    // noop
  }
}
