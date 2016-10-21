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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.remote.transport.Link;

import java.net.SocketAddress;

/**
 * Event sent from a remote node.
 */
public class TransportEvent {

  private final byte[] data;
  private final SocketAddress localAddr;
  private final SocketAddress remoteAddr;
  private final Link<byte[]> link;

  /**
   * Constructs an object event.
   *
   * @param data       the data
   * @param localAddr  the local socket address
   * @param remoteAddr the remote socket address
   */
  public TransportEvent(final byte[] data, final SocketAddress localAddr, final SocketAddress remoteAddr) {
    this.data = data;
    this.localAddr = localAddr;
    this.remoteAddr = remoteAddr;
    link = null;
  }

  /**
   * Constructs the transport even object using link to.
   * initialize local and remote address if link not null
   *
   * @param data
   * @param link
   */
  public TransportEvent(final byte[] data, final Link<byte[]> link) {
    this.data = data;
    this.link = link;
    if (this.link != null) {
      localAddr = link.getLocalAddress();
      remoteAddr = link.getRemoteAddress();
    } else {
      localAddr = null;
      remoteAddr = null;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "TransportEvent: {local: %s remote: %s size: %d bytes}",
        this.localAddr, this.remoteAddr, this.data.length);
  }

  /**
   * Gets the data.
   *
   * @return data
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Returns the link associated with the event.
   * which can be used to write back to the client
   * without creating a new link
   *
   * @return an existing link associated with the event
   */
  public Link<byte[]> getLink() {
    return link;
  }

  /**
   * Gets the local socket address.
   *
   * @return the local socket address
   */
  public SocketAddress getLocalAddress() {
    return localAddr;
  }

  /**
   * Gets the remote socket address.
   *
   * @return the remote socket address
   */
  public SocketAddress getRemoteAddress() {
    return remoteAddr;
  }
}
