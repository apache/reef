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
package org.apache.reef.io.network.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.transport.Link;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class NetworkConnection<T> implements Connection<T> {

  private Link<NetworkConnectionServiceMessage<T>> link;

  private final Identifier destId;
  private final AtomicBoolean closed;
  private final NetworkConnectionFactory<T> connFactory;

  /**
   * Constructs a connection for destination identifier of NetworkConnectionService.
   * @param connFactory a connection factory of this connection.
   * @param destId a destination identifier of NetworkConnectionService.
   */
  NetworkConnection(
      final NetworkConnectionFactory<T> connFactory,
      final Identifier destId) {
    this.connFactory = connFactory;
    this.destId = destId;
    this.closed = new AtomicBoolean();
  }

  @Override
  public void open() throws NetworkException {
    link = connFactory.openLink(destId);
  }

  @Override
  public void write(final List<T> messageList) {
    final NetworkConnectionServiceMessage<T> nsMessage = new NetworkConnectionServiceMessage<>(
        connFactory.getConnectionFactoryId().toString(),
        connFactory.getLocalEndPointId(),
        destId,
        messageList);
    link.write(nsMessage);
  }

  @Override
  public void write(final T message) {
    final List<T> messageList = new ArrayList<>(1);
    messageList.add(message);
    write(messageList);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      connFactory.removeConnection(this.destId);
      link = null;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Connection from")
        .append(connFactory.getLocalEndPointId())
        .append(":")
        .append(connFactory.getConnectionFactoryId())
        .append(" to ")
        .append(destId)
        .append(":")
        .append(connFactory.getConnectionFactoryId());
    return sb.toString();
  }
}
