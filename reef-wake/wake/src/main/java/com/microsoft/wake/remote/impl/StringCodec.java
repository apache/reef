/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.impl;

import com.microsoft.wake.remote.Codec;

/**
 * Codec that encodes/decodes a string
 */
public class StringCodec implements Codec<String> {

  /**
   * Returns a byte array representation of the string
   * 
   * @param obj the string
   * @return a byte array representation of the string
   */
  @Override
  public byte[] encode(String obj) {
    return obj.getBytes();
  }

  /**
   * Returns a string decoded from the byte array
   * 
   * @param buf the byte array
   * @return a string
   */
  @Override
  public String decode(byte[] buf) {
    return new String(buf);
  }
}

