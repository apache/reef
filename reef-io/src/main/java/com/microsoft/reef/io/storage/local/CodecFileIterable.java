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

/**
 * A read-only spool implementation, based on files. Some other process needs to
 * create the spool file for us.
 * 
 * @author sears
 * 
 */
public class CodecFileIterable<T, C extends Codec<T>> implements Iterable<T> {
  final File filename;
  final C codec;

  public CodecFileIterable(File filename, C codec) {
    this.filename = filename;
    this.codec = codec;
  }

  @SuppressWarnings("resource")
  @Override
  public Iterator<T> iterator() {
    try {
      final ObjectInputStream in;
      in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
          filename)));
      return new Iterator<T>() {
        int sz = in.readInt();

        @Override
        public boolean hasNext() {
          return sz != -1;
        }

        @Override
        public T next() {
          try {
          byte[] buf = new byte[sz];
          for (int rem = buf.length; rem > 0; rem -= in.read(buf, buf.length
              - rem, rem)) {
          }
          sz = in.readInt();
          if (sz == -1) {
            in.close();
          }
          return codec.decode(buf);
          } catch (IOException e) {
            throw new ServiceRuntimeException(new StorageException(e));
          }
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException(
              "Attempt to remove value from read-only input file!");
        }
      };
    } catch (IOException e) {
      throw new ServiceRuntimeException(new StorageException(e));
    }
  }


}
