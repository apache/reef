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
package org.apache.reef.io.network;

import org.apache.reef.wake.Identifier;

import java.net.SocketAddress;
import java.util.List;

/**
 * Event holding Identifier information
 *
 * @param <T> type
 */
public final class NetworkEvent<T> {

  private final List<T> eventList;
  private final SocketAddress remoteAddr;
  private final Identifier remoteId;

  /**
   * Constructs a network event
   *
   * @param remoteAddr the remote socket address
   * @param remoteId   the remote identifier
   * @param eventList  the list of events
   */
  public NetworkEvent(
      final SocketAddress remoteAddr,
      final Identifier remoteId,
      final List<T> eventList) {
    this.remoteAddr = remoteAddr;
    this.remoteId = remoteId;
    this.eventList = eventList;
  }

  /**
   * Gets the remote socket address.
   *
   * @return the remote socket address
   */
  public SocketAddress getRemoteAddress() {
    return remoteAddr;
  }


  /**
   * Gets the remote identifier.
   * If user configured not to provide sender's identifier, it returns null
   *
   * @return the remote id
   */
  public Identifier getRemoteId() {
    return remoteId;
  }

  /**
   * Returns the event at the index of list.
   * If index is bigger than size, it returns null.
   *
   * @param index
   * @return event at index
   */
  public T getEventAt(int index) {
    if (index >= eventList.size()) {
      return null;
    }
    return eventList.get(index);
  }

  /**
   * Returns size of event list
   *
   * @return event list size
   */
  public int getEventListSize() {
    return eventList.size();
  }

  /**
   * Returns a string representation of this object
   *
   * @return a string representation of this object
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("NetworkEvent");
    builder.append(" remoteID=");
    builder.append(remoteId);
    builder.append(" event=[| ");
    for (T event : eventList) {
      builder.append(event + " |");
    }
    builder.append("]");
    return builder.toString();
  }
}

