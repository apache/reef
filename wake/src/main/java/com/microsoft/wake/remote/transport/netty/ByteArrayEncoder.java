/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.transport.netty;

import static org.jboss.netty.buffer.ChannelBuffers.*;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Netty byte array encoder
 */
public class ByteArrayEncoder extends OneToOneEncoder {

  /**
   * Encodes a message into a channel buffer
   * 
   * @param ctx a channel handler context
   * @param channel a channel
   * @param msg a message
   * @return the channel buffer of the message if the message is a byte array; the message, otherwise
   */
  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
      throws Exception {
    if (!(msg instanceof byte[])) {
      return msg;
    }
    return wrappedBuffer((byte[]) msg);
  }

}
