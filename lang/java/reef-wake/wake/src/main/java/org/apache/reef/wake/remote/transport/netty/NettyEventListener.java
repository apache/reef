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

import io.netty.channel.ChannelHandlerContext;

/**
 * Netty event listener.
 */
interface NettyEventListener {

  /**
   * Handles the message.
   *
   * @param ctx the channel handler context
   * @param msg the message
   */
  void channelRead(ChannelHandlerContext ctx, Object msg);

  /**
   * Handles the exception event.
   *
   * @param ctx   the channel handler context
   * @param cause the cause
   */
  void exceptionCaught(ChannelHandlerContext ctx, Throwable cause);

  /**
   * Handles the channel active event.
   *
   * @param ctx the channel handler context
   */
  void channelActive(ChannelHandlerContext ctx);

  /**
   * Handles the channel inactive event.
   *
   * @param ctx the channel handler context
   */
  void channelInactive(ChannelHandlerContext ctx);
}
