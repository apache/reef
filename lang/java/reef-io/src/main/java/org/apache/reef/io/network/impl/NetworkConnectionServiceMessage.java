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

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.Identifier;

import java.net.SocketAddress;
import java.util.List;


/**
 * NetworkConnectionServiceMessage implementation.
 * This is a wrapper message of message type <T>.
 */
final class NetworkConnectionServiceMessage<T> implements Message<T> {

  private final List<T> messages;
  private SocketAddress remoteAddr;
  private final String connFactoryId;
  private final Identifier srcId;
  private final Identifier destId;

  /**
   * Constructs a network connection service message.
   *
   * @param connFactoryId the connection factory identifier
   * @param srcId      the source identifier of NetworkConnectionService
   * @param destId   the destination identifier of NetworkConnectionService
   * @param messages  the list of messages
   */
  NetworkConnectionServiceMessage(
      final String connFactoryId,
      final Identifier srcId,
      final Identifier destId,
      final List<T> messages) {
    this.connFactoryId = connFactoryId;
    this.srcId = srcId;
    this.destId = destId;
    this.messages = messages;
  }

  void setRemoteAddress(final SocketAddress remoteAddress) {
    this.remoteAddr = remoteAddress;
  }

  /**
   * Gets a destination identifier.
   *
   * @return a remote id
   */
  @Override
  public Identifier getDestId() {
    return destId;
  }

  /**
   * Gets a connection factory identifier.
   *
   * @return a connection factory id
   */
  public String getConnectionFactoryId() {
    return connFactoryId;
  }


  /**
   * Gets a source identifier of NetworkConnectionService.
   *
   * @return a source id
   */
  @Override
  public Identifier getSrcId() {
    return srcId;
  }

  @Override
  public List<T> getData() {
    return messages;
  }

  /**
   * Returns a string representation of this object.
   *
   * @return a string representation of this object
   */
  public String toString() {
    return String.format("%s[%s -> %s]: size %d",
        this.getClass().getSimpleName(), this.srcId, this.destId, this.messages.size());
  }
}
