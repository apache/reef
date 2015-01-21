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

import org.apache.reef.wake.remote.Codec;

/**
 * Codec that performs identity transformation on bytes
 */
public class ByteCodec implements Codec<byte[]> {

  /**
   * Returns the byte array argument
   *
   * @param obj bytes
   * @return the same bytes
   */
  @Override
  public byte[] encode(byte[] obj) {
    return obj;
  }

  /**
   * Returns the byte array argument
   *
   * @param buf bytes
   * @return the same bytes
   */
  @Override
  public byte[] decode(byte[] buf) {
    return buf;
  }

}
