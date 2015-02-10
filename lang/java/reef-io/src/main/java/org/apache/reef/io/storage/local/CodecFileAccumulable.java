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
package org.apache.reef.io.storage.local;

import org.apache.reef.exception.evaluator.StorageException;
import org.apache.reef.io.Accumulable;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.serialization.Codec;

import java.io.File;
import java.io.IOException;

public final class CodecFileAccumulable<T, C extends Codec<T>> implements Accumulable<T> {

  private final File filename;
  private final C codec;

  public CodecFileAccumulable(final LocalStorageService s, final C codec) {
    this.filename = s.getScratchSpace().newFile();
    this.codec = codec;
  }

  public String getName() {
    return this.filename.toString();
  }

  @Override
  public Accumulator<T> accumulator() throws StorageException {
    try {
      return new CodecFileAccumulator<>(this.codec, this.filename);
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

}
