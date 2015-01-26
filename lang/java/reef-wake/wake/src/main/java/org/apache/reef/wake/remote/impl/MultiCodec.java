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
import org.apache.reef.wake.remote.Decoder;
import org.apache.reef.wake.remote.Encoder;

import java.util.HashMap;
import java.util.Map;

/**
 * Codec using the WakeTuple protocol buffer
 * (class name and bytes)
 *
 * @param <T>
 */
public class MultiCodec<T> implements Codec<T> {

  private final Encoder<T> encoder;
  private final Decoder<T> decoder;

  /**
   * Constructs a codec that encodes/decodes an object to/from bytes based on the class name
   *
   * @param clazzToDecoderMap
   */
  public MultiCodec(Map<Class<? extends T>, Codec<? extends T>> clazzToCodecMap) {
    Map<Class<? extends T>, Encoder<? extends T>> clazzToEncoderMap = new HashMap<Class<? extends T>, Encoder<? extends T>>();
    Map<Class<? extends T>, Decoder<? extends T>> clazzToDecoderMap = new HashMap<Class<? extends T>, Decoder<? extends T>>();
    for (Class<? extends T> clazz : clazzToCodecMap.keySet()) {
      clazzToEncoderMap.put(clazz, clazzToCodecMap.get(clazz));
      clazzToDecoderMap.put(clazz, clazzToCodecMap.get(clazz));
    }
    encoder = new MultiEncoder<T>(clazzToEncoderMap);
    decoder = new MultiDecoder<T>(clazzToDecoderMap);
  }

  /**
   * Encodes an object to a byte array
   *
   * @param obj
   */
  @Override
  public byte[] encode(T obj) {
    return encoder.encode(obj);
  }

  /**
   * Decodes byte array
   *
   * @param data class name and byte payload
   */
  @Override
  public T decode(byte[] data) {
    return decoder.decode(data);
  }

}
