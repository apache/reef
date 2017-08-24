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
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.URI;

/**
 * Factory that creates a NettyHttpLink.
 */
public final class NettyHttpLinkFactory<T> implements NettyLinkFactory {

  private final URI uri;

  NettyHttpLinkFactory(final URI uri){
    this.uri = uri;
  }

  @Override
  public Link newInstance(final Channel channel, final Encoder encoder) {
    return new NettyHttpLink<T>(channel, encoder, uri);
  }

  @Override
  public Link newInstance(final Channel channel,
                          final Encoder encoder,
                          final LinkListener listener) {
    return new NettyHttpLink<T>(channel, encoder, listener, uri);
  }
}
