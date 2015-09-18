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
package org.apache.reef.io.storage.util;

import org.apache.reef.io.serialization.Codec;

import java.nio.charset.StandardCharsets;

public class IntegerCodec implements Codec<Integer> {

  @Override
  public byte[] encode(final Integer obj) {
    return Integer.toString(obj).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Integer decode(final byte[] buf) {
    return Integer.decode(new String(buf, StandardCharsets.UTF_8));
  }

}
