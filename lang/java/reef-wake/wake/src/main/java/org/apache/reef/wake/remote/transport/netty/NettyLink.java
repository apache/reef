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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Link implementation with Netty.
 *
 * If you set a {@code LinkListener<T>}, it keeps message until writeAndFlush operation completes
 * and notifies whether the sent message transferred successfully through the listener.
 */
public class NettyLink<T> implements Link<T> {

  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  private static final Logger LOG = Logger.getLogger(NettyLink.class.getName());

  private final Channel channel;
  private final Encoder<? super T> encoder;
  private final LinkListener<? super T> listener;

  /**
   * Constructs a link.
   *
   * @param channel the channel
   * @param encoder the encoder
   */
  public NettyLink(final Channel channel, final Encoder<? super T> encoder) {
    this(channel, encoder, null);
  }

  /**
   * Constructs a link.
   *
   * @param channel  the channel
   * @param encoder  the encoder
   * @param listener the link listener
   */
  public NettyLink(final Channel channel, final Encoder<? super T> encoder, final LinkListener<? super T> listener) {
    this.channel = channel;
    this.encoder = encoder;
    this.listener = listener;
  }

  /**
   * Writes the message to this link.
   *
   * @param message the message
   */
  @Override
  public void write(final T message) {
    LOG.log(Level.FINEST, "write {0} :: {1}", new Object[] {channel, message});
    final ChannelFuture future = channel.writeAndFlush(Unpooled.wrappedBuffer(encoder.encode(message)));
    if (listener !=  null) {
      future.addListener(new NettyChannelFutureListener<>(message, listener));
    }
  }

  /**
   * Gets a local address of the link.
   *
   * @return a local socket address
   */
  @Override
  public SocketAddress getLocalAddress() {
    return channel.localAddress();
  }

  /**
   * Gets a remote address of the link.
   *
   * @return a remote socket address
   */
  @Override
  public SocketAddress getRemoteAddress() {
    return channel.remoteAddress();
  }

  @Override
  public String toString() {
    return "NettyLink: " + channel; // Channel has good .toString() implementation
  }
}

class NettyChannelFutureListener<T> implements ChannelFutureListener {

  private final T message;
  private LinkListener<T> listener;

  NettyChannelFutureListener(final T message, final LinkListener<T> listener) {
    this.message = message;
    this.listener = listener;
  }

  @Override
  public void operationComplete(final ChannelFuture channelFuture) throws Exception {
    if (channelFuture.isSuccess()) {
      listener.onSuccess(message);
    } else {
      listener.onException(channelFuture.cause(), channelFuture.channel().remoteAddress(), message);
    }
  }
}
