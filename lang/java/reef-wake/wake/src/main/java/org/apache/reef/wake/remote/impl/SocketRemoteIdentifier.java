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

import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import java.net.InetSocketAddress;

/**
 * Remote identifier based on a socket address.
 */
public class SocketRemoteIdentifier implements RemoteIdentifier {

  private InetSocketAddress addr;

  /**
   * Constructs a remote identifier.
   *
   * @param addr the socket address
   */
  public SocketRemoteIdentifier(final InetSocketAddress addr) {
    this.addr = addr;
  }

  /**
   * Constructs a remote identifier.
   *
   * @param str the string representation
   * @throws RemoteRuntimeException
   */
  public SocketRemoteIdentifier(final String str) {
    int index = str.indexOf("0:0:0:0:0:0:0:0:");

    if (index >= 0) {
      final String host = str.substring(0, 15);
      final int port = Integer.parseInt(str.substring(index + 16));
      this.addr = new InetSocketAddress(host, port);
    } else {
      index = str.indexOf(":");
      if (index <= 0) {
        throw new RemoteRuntimeException("Invalid name " + str);
      }
      final String host = str.substring(0, index);
      final int port = Integer.parseInt(str.substring(index + 1));
      this.addr = new InetSocketAddress(host, port);
    }
  }

  /**
   * Gets the socket address.
   *
   * @return the socket address
   */
  public InetSocketAddress getSocketAddress() {
    return addr;
  }

  /**
   * Returns a hash code for the object.
   *
   * @return a hash code value for this object
   */
  @Override
  public int hashCode() {
    return addr.hashCode();
  }

  /**
   * Checks that another object is equal to this object.
   *
   * @param o another object
   * @return true if the object is the same as the object argument; false, otherwise
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return addr.equals(((SocketRemoteIdentifier) o).getSocketAddress());
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("socket://");
    builder.append(addr.getHostString());
    builder.append(":");
    builder.append(addr.getPort());
    return builder.toString();
  }

}
