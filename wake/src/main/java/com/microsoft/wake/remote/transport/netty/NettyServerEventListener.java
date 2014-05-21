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

import com.microsoft.wake.EStage;
import com.microsoft.wake.remote.impl.ByteCodec;
import com.microsoft.wake.remote.impl.TransportEvent;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

final class NettyServerEventListener extends AbstractNettyEventListener {

  public NettyServerEventListener(
      final ConcurrentMap<SocketAddress, LinkReference> addrToLinkRefMap,
      final EStage<TransportEvent> stage) {
    super(addrToLinkRefMap, stage);
  }

  @Override
  protected TransportEvent getTransportEvent(final byte[] message, final Channel channel) {
    return new TransportEvent(message, new NettyLink<>(channel, new ByteEncoder()));
  }

  @Override
  public void channelConnected(final ChannelStateEvent event) {

    final Channel channel = event.getChannel();

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "Channel connected. key: {0} :: {1}", new Object[] {
          channel.getRemoteAddress(), event });
    }

    final LinkReference ref = this.addrToLinkRefMap.putIfAbsent(
        channel.getRemoteAddress(), new LinkReference(new NettyLink<>(
        channel, new ByteCodec(), new LoggingLinkListener<byte[]>())));

    LOG.log(Level.FINER, "Add connected channel ref: {0}", ref);
  }

  @Override
  protected void exceptionCleanup(final ExceptionEvent event) {
    // noop
  }
}
