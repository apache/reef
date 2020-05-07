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
package org.apache.reef.runtime.common.utils;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

/**
 * (De-)serializes exceptions.
 */
@DefaultImplementation(DefaultExceptionCodec.class)
public interface ExceptionCodec {

  /**
   * Deserializes a Throwable that has been serialized using toBytes().
   *
   * @param bytes
   * @return the Throwable or Optional.empty() if the deserialization fails.
   */
  Optional<Throwable> fromBytes(byte[] bytes);

  /**
   * @param bytes
   * @return fromBytes(bytes.get()) or Optional.empty()
   */
  Optional<Throwable> fromBytes(Optional<byte[]> bytes);

  /**
   * @param throwable
   * @return the serialized form of the given Throwable.
   */
  byte[] toBytes(Throwable throwable);

}
