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

import org.apache.reef.exception.evaluator.ServiceRuntimeException;
import org.apache.reef.exception.evaluator.StorageException;
import org.apache.reef.io.serialization.Codec;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * A read-only spool implementation, based on files. Some other process needs to
 * create the spool file for us.
 */
public class CodecFileIterable<T, C extends Codec<T>> implements Iterable<T> {
  private final File filename;
  private final C codec;

  public CodecFileIterable(final File filename, final C codec) {
    this.filename = filename;
    this.codec = codec;
  }

  @SuppressWarnings("resource")
  @Override
  public Iterator<T> iterator() {
    try {
      return new CodecFileIterator<>(this.codec, this.filename);
    } catch (final IOException e) {
      throw new ServiceRuntimeException(new StorageException(e));
    }
  }


}
