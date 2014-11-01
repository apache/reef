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

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.Identifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Network service message that implements the Message interface
 *
 * @param <T> type
 */
public class NSMessage<T> implements Message<T> {
  private final Identifier srcId;
  private final Identifier destId;
  private final List<T> data;

  /**
   * Constructs a network service message
   *
   * @param srcId  a source identifier
   * @param destId a destination identifier
   * @param data   data of type T
   */
  public NSMessage(final Identifier srcId, final Identifier destId, final T data) {
    this.srcId = srcId;
    this.destId = destId;
    this.data = new ArrayList<T>(1);
    this.data.add(data);
  }

  /**
   * Constructs a network service message
   *
   * @param srcId  a source identifier
   * @param destId a destination identifier
   * @param data   a list of data of type T
   */
  public NSMessage(final Identifier srcId, final Identifier destId, final List<T> data) {
    this.srcId = srcId;
    this.destId = destId;
    this.data = data;
  }

  /**
   * Gets a source identifier
   *
   * @return an identifier
   */
  public Identifier getSrcId() {
    return srcId;
  }

  /**
   * Gets a destination identifier
   *
   * @return an identifier
   */
  public Identifier getDestId() {
    return destId;
  }

  /**
   * Gets data
   *
   * @return data
   */
  public List<T> getData() {
    return data;
  }
}
