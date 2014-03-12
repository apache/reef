/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.exception.evaluator.ServiceRuntimeException;
import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.serialization.Codec;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

final class CodecFileIterator<T> implements Iterator<T> {

  private static final Logger LOG = Logger.getLogger(CodecFileIterator.class.getName());

  private final Codec<T> codec;
  private final ObjectInputStream in;
  private int sz = -1;

  CodecFileIterator(final Codec<T> codec, final File file) throws IOException {
    this.in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    this.codec = codec;
    try {
      this.sz = this.in.readInt();
    } catch (final IOException ex) {
      this.in.close();
      throw ex;
    }
  }

  @Override
  public boolean hasNext() {
    return this.sz != -1;
  }

  @Override
  public T next() {
    if (this.sz == -1) {
      throw new NoSuchElementException("Moving past the end of the file.");
    }
    try {
      final byte[] buf = new byte[this.sz];
      for (int rem = buf.length; rem > 0; rem -= this.in.read(buf, buf.length - rem, rem)) {
      }
      this.sz = this.in.readInt();
      if (this.sz == -1) {
        this.in.close();
      }
      return this.codec.decode(buf);
    } catch (final IOException e) {
      this.sz = -1; // Don't read from that file again.
      try {
        this.in.close();
      } catch (final IOException exClose) {
        LOG.log(Level.WARNING, "Error closing file.", exClose);
      }
      throw new ServiceRuntimeException(new StorageException(e));
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Attempt to remove value from read-only input file!");
  }
}
