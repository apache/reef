/**
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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.EventHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Remote receiver event handler
 */
class RemoteReceiverEventHandler implements EventHandler<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(RemoteReceiverEventHandler.class.getName());

  private final RemoteEventCodec<byte[]> codec;
  private final EventHandler<RemoteEvent<byte[]>> handler;

  /**
   * Constructs a remote receiver event handler
   *
   * @param handler the upstream handler
   */
  RemoteReceiverEventHandler(EventHandler<RemoteEvent<byte[]>> handler) {
    this.codec = new RemoteEventCodec<byte[]>(new ByteCodec());
    this.handler = handler;
  }

  /**
   * Handles the event received from a remote node
   *
   * @param e the event
   */
  @Override
  public void onNext(TransportEvent e) {
    RemoteEvent<byte[]> re = codec.decode(e.getData());
    re.setLocalAddress(e.getLocalAddress());
    re.setRemoteAddress(e.getRemoteAddress());

    if (LOG.isLoggable(Level.FINER))
      LOG.log(Level.FINER, "{0} {1}", new Object[]{e, re});
    handler.onNext(re);
  }
}
