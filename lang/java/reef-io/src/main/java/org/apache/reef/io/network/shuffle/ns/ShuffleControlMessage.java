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
package org.apache.reef.io.network.shuffle.ns;

/**
 *
 */
public final class ShuffleControlMessage extends ShuffleMessage {

  private final byte[][] data;

  public ShuffleControlMessage(
      final int code,
      final String topologyName,
      final String groupingName,
      final byte[][] data) {
    super(code, topologyName, groupingName);
    if (code == ShuffleMessage.TUPLE_MESSAGE) {
      throw new RuntimeException("0 cannot be used as ShuffleControlMessage code since it is reserved for TUPLE_MESSAGE");
    }
    this.data = data;
  }

  @Override
  public int getDataLength() {
    if (data == null) {
      return 0;
    }

    return data.length;
  }

  @Override
  public byte[] getDataAt(final int index) {
    return data[index];
  }
}
