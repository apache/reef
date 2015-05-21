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
package org.apache.reef.io.network.impl;

import java.net.SocketAddress;
import java.util.List;

/**
 * Wraps encoded event data and a class name key used in finding event handlers
 * and codecs for that data.
 */
public final class NetworkServiceEvent<T> {
  private final int eventClassNameCode;
  private final List<T> dataList;
  private SocketAddress localAddr;
  private SocketAddress remoteAddr;
  private final String remoteId;
  private final String localId;

  NetworkServiceEvent(final int eventClassNameCode, final List<T> dataList,
                      final String remoteId, final String localId) {
    this.eventClassNameCode = eventClassNameCode;
    this.remoteId = remoteId;
    this.localId = localId;
    this.dataList = dataList;
  }

  @Override
  public String toString() {
    return "NetworkServiceEvent : hashCode : " + eventClassNameCode + ", remoteId :" + remoteId;
  }

  /**
   * Returns an event class name hash code
   *
   * @return an event class name hash code
   */
  int getEventClassNameCode() {
    return eventClassNameCode;
  }

  /**
   * Returns list of encoded data byte array
   *
   * @return list of encoded data byte array
   */
  List<T> getDataList() {
    return dataList;
  }

  /**
   * Returns a remote identifier
   *
   * @return a remote identifier string
   */
  String getRemoteId() {
    return remoteId;
  }

  /**
   * Returns a local identifier
   *
   * @return a local identifier string
   */
  String getLocalId() {
    return localId;
  }

  /**
   * Gets the local socket address
   *
   * @return the local socket address
   */
  SocketAddress localAddress() {
    return localAddr;
  }

  /**
   * Gets the remote socket address
   *
   * @return the remote socket address
   */
  SocketAddress remoteAddress() {
    return remoteAddr;
  }

  /**
   * Sets the local socket address
   *
   * @param addr the local socket address
   */
  void setLocalAddress(SocketAddress addr) {
    localAddr = addr;
  }

  /**
   * Sets the remote socket address
   *
   * @param addr the remote socket address
   */
  void setRemoteAddress(SocketAddress addr) {
    remoteAddr = addr;
  }
}
