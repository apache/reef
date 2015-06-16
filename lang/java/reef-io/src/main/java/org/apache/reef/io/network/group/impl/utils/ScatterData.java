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
package org.apache.reef.io.network.group.impl.utils;

import java.util.Map;

/**
 * Represents data that is transferred across evaluators during Scatter.
 */
public final class ScatterData {

  /**
   * Data that is assigned to this node.
   */
  private final byte[][] myData;

  /**
   * A map of data that is assigned to this node's chidren.
   */
  private final Map<String, byte[]> childrenData;

  public ScatterData(final byte[][] myData, final Map<String, byte[]> childrenData) {
    this.myData = myData;
    this.childrenData = childrenData;
  }

  public byte[][] getMyData() {
    return this.myData;
  }

  public Map<String, byte[]> getChildrenData() {
    return this.childrenData;
  }
}
