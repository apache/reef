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

import com.microsoft.reef.exception.evaluator.StorageException;
import com.microsoft.reef.io.Accumulable;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.serialization.Codec;

import java.io.*;
import java.util.ConcurrentModificationException;

/**
 * @deprecated Please don't use this code. It isn't safe and might leak file handles.
 */
public class CodecFileAccumulable<T, C extends Codec<T>> implements Accumulable<T> {
  private final File filename;
  private final C codec;

  public CodecFileAccumulable(LocalStorageService s, C codec) {
    this.filename = s.getScratchSpace().newFile();
    this.codec = codec;
  }

  public String getName() {
    return filename.toString();
  }

  @Override
  public Accumulator<T> accumulator() throws StorageException {
    try {
      return new Accumulator<T>() {
        ObjectOutputStream out = new ObjectOutputStream(
            new BufferedOutputStream(new FileOutputStream(filename)));

        @Override
        public void add(T datum) throws StorageException {
          if (out == null) {
            throw new ConcurrentModificationException(
                "Attempt to write to accumulator after done()");
          }
            byte[] buf = codec.encode(datum);
            try {
              out.writeInt(buf.length);
              out.write(buf);
            } catch (IOException e) {
              throw new StorageException(e);
            }
        }

        @Override
        public void close() throws StorageException {
          try {
            out.writeInt(-1);
            out.close();
            out = null;
          } catch (Exception e) {
            throw new StorageException(e);
          }

        }
      };
    } catch (IOException e) {
      throw new StorageException(e);
    }
  }

}
