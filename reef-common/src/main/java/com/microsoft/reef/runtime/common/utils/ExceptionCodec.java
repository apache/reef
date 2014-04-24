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
package com.microsoft.reef.runtime.common.utils;

import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * (De-)serializes exceptions.
 */
@DefaultImplementation(DefaultExceptionCodec.class)
public interface ExceptionCodec {

  /**
   * Deserializes a Throwable that has been serialized using toBytes().
   *
   * @param bytes
   * @return the Throable or Optional.empty() if the deserialization fails.
   */
  public Optional<Throwable> fromBytes(final byte[] bytes);

  /**
   * @param bytes
   * @return fromBytes(bytes.get()) or Optional.empty()
   */
  public Optional<Throwable> fromBytes(final Optional<byte[]> bytes);

  /**
   * @param throwable
   * @return the serialized form of the given Throwable.
   */
  public byte[] toBytes(final Throwable throwable);

}
