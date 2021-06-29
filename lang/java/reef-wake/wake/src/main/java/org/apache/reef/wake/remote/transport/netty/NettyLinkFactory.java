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
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 * Factory that creates a NettyLink.
 */
@DefaultImplementation(NettyDefaultLinkFactory.class)
public interface NettyLinkFactory<T> {

  /**
   * Creates a NettyLink.
   * @param channel the channel
   * @param encoder the encoder
   */
  Link<T> newInstance(final Channel channel, final Encoder<? super T> encoder);

  /**
   * Creates a NettyLink.
   * @param channel  the channel
   * @param encoder  the encoder
   * @param listener the listener
   */
  Link<T> newInstance(final Channel channel,
                      final Encoder<? super T> encoder,
                      final LinkListener<? super T> listener);
}
