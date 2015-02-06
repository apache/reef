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

import java.net.SocketAddress;

/**
 * Event that are exchanged across process boundaries
 *
 * @param <T> type
 */
public class RemoteEvent<T> {

  private final T event;
  private final long seq;
  //private static final AtomicLong curSeq = new AtomicLong(0);
  private SocketAddress localAddr;
  private SocketAddress remoteAddr;
  private String src;
  private String sink;

  /**
   * Constructs a remote event
   *
   * @param localAddr  the local socket address
   * @param remoteAddr the remote socket address
   * @param src        the source
   * @param sink       the remote sink
   * @param seq        the sequence number
   * @param event      the event
   */
  public RemoteEvent(SocketAddress localAddr, SocketAddress remoteAddr, String src, String sink, long seq, T event) {
    this.localAddr = localAddr;
    this.remoteAddr = remoteAddr;
    this.src = src;
    this.sink = sink;
    this.event = event;
    this.seq = seq;
  }

  /**
   * Gets the local socket address
   *
   * @return the local socket address
   */
  public SocketAddress localAddress() {
    return localAddr;
  }

  /**
   * Gets the remote socket address
   *
   * @return the remote socket address
   */
  public SocketAddress remoteAddress() {
    return remoteAddr;
  }

  /**
   * Gets the source
   *
   * @return the source
   */
  public String getSource() {
    return src;
  }

  /**
   * Sets the source
   *
   * @param name the source name
   */
  public void setSource(String name) {
    src = name;
  }

  /**
   * Gets the sink
   *
   * @return the sink
   */
  public String getSink() {
    return sink;
  }

  /**
   * Sets the sink
   *
   * @param name the sink name
   */
  public void setSink(String name) {
    sink = name;
  }

  /**
   * Gets the actual event
   *
   * @return the event
   */
  public T getEvent() {
    return event;
  }

  /**
   * Gets the sequence number
   *
   * @return the sequence number
   */
  public long getSeq() {
    return seq;
  }

  /**
   * Sets the local socket address
   *
   * @param addr the local socket address
   */
  public void setLocalAddress(SocketAddress addr) {
    localAddr = addr;
  }

  /**
   * Sets the remote socket address
   *
   * @param addr the remote socket address
   */
  public void setRemoteAddress(SocketAddress addr) {
    remoteAddr = addr;
  }

  /**
   * Returns a string representation of this object
   *
   * @return a string representation of this object
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("RemoteEvent");
    builder.append(" localAddr=");
    builder.append(localAddr);
    builder.append(" remoteAddr=");
    builder.append(remoteAddr);
    builder.append(" sourceName=");
    builder.append(src);
    builder.append(" sinkName=");
    builder.append(sink);
    builder.append(" seq=");
    builder.append(seq);
    builder.append(" event=");
    builder.append(event);
    return builder.toString();
  }

}
