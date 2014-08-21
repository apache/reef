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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.LinkListener;

/**
 * Link implementation with Netty
 */
public class NettyLink<T> implements Link<T> {

  private static final Logger LOG = Logger.getLogger(NettyLink.class.getName());

  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  private final Channel channel;
  private final Encoder<? super T> encoder;
  private final LinkListener<? super T> listener;

  /**
   * Constructs a link
   *
   * @param channel the channel
   * @param encoder the encoder
   */
  public NettyLink(final Channel channel, final Encoder<? super T> encoder) {
    this(channel, encoder, null);
  }

  /**
   * Constructs a link
   *
   * @param channel the channel
   * @param encoder the encoder
   * @param listener the link listener
   */
  public NettyLink(final Channel channel,
        final Encoder<? super T> encoder, final LinkListener<? super T> listener) {
    this.channel = channel;
    this.encoder = encoder;
    this.listener = listener;
  }


  /**
   * Writes the message to this link
   *
   * @param message the message
   */
  @Override
  public void write(final T message) throws IOException {
    LOG.log(Level.FINEST, "write {0} {1}", new Object[]{channel, message});
    byte[] allData = encoder.encode(message);
    // byte[] -> ByteBuf
    channel.writeAndFlush(Unpooled.wrappedBuffer(allData));
  }

  /**
   * Handles the message received
   *
   * @param message the message
   */
  @Override
  public void messageReceived(final T message) {
    if (listener != null) {
      listener.messageReceived(message);
    }
  }

  /**
   * Gets a local address of the link
   *
   * @return a local socket address
   */
  @Override
  public SocketAddress getLocalAddress() {
    return channel.localAddress();
  }

  /**
   * Gets a remote address of the link
   *
   * @return a remote socket address
   */
  @Override
  public SocketAddress getRemoteAddress() {
    return channel.remoteAddress();
  }

  @Override
  public String toString() {
    return "localAddr: " + getLocalAddress() + " remoteAddr: " + getRemoteAddress();
  }
}
