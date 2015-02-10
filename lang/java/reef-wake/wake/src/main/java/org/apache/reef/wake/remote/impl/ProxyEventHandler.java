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
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Proxy of the event handler that runs remotely
 *
 * @param <T> type
 */
public class ProxyEventHandler<T> implements EventHandler<T> {
  private static final Logger LOG = Logger.getLogger(ProxyEventHandler.class.getName());

  private final SocketRemoteIdentifier myId;
  private final SocketRemoteIdentifier remoteId;
  private final String remoteSinkName;
  private final EventHandler<RemoteEvent<T>> handler;
  private final RemoteSeqNumGenerator seqGen;

  /**
   * Constructs a proxy event handler
   *
   * @param myId           my identifier
   * @param remoteId       the remote identifier
   * @param remoteSinkName the remote sink name
   * @param reStage        the sender stage
   * @throws RemoteRuntimeException
   */
  public ProxyEventHandler(RemoteIdentifier myId, RemoteIdentifier remoteId, String remoteSinkName, EventHandler<RemoteEvent<T>> handler, RemoteSeqNumGenerator seqGen) {
    LOG.log(Level.FINE, "ProxyEventHandler myId: {0} remoteId: {1} remoteSink: {2} handler: {3}", new Object[]{myId, remoteId, remoteSinkName, handler});
    if (!(myId instanceof SocketRemoteIdentifier && remoteId instanceof SocketRemoteIdentifier)) {
      throw new RemoteRuntimeException("Unsupported remote identifier type");
    }

    this.myId = (SocketRemoteIdentifier) myId;
    this.remoteId = (SocketRemoteIdentifier) remoteId;
    this.remoteSinkName = remoteSinkName;
    this.handler = handler;
    this.seqGen = seqGen;
  }

  /**
   * Sends the event to the event handler running remotely
   *
   * @param event the event
   */
  @Override
  public void onNext(T event) {
    if (LOG.isLoggable(Level.FINE))
      LOG.log(Level.FINE, "remoteid: {0}\n{1}", new Object[]{remoteId.getSocketAddress(), event.toString()});
    handler.onNext(new RemoteEvent<T>(myId.getSocketAddress(), remoteId.getSocketAddress(), "", remoteSinkName,
        seqGen.getNextSeq(remoteId.getSocketAddress()), event));
  }

  /**
   * Returns a string representation of the object
   *
   * @return a string representation of the object
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass().getName());
    builder.append(" remote_id=");
    builder.append(remoteId.toString());
    return builder.toString();
  }
}
