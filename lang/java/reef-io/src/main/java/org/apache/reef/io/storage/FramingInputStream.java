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
package org.apache.reef.io.storage;

import org.apache.reef.exception.evaluator.ServiceRuntimeException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class FramingInputStream extends DataInputStream implements Iterable<byte[]> {

  public FramingInputStream(InputStream in) {
    super(in);
  }

  public byte[] readFrame() throws IOException {
    int i = readInt();
    if (i == -1) {
      return null;
    }
    byte[] b = new byte[i];
    readFully(b);
    return b;
  }

  @Override
  public Iterator<byte[]> iterator() {
    try {
      return new Iterator<byte[]>() {
        byte[] cur = readFrame();

        @Override
        public boolean hasNext() {
          return cur != null;
        }

        @Override
        public byte[] next() {
          byte[] ret = cur;
          try {
            cur = readFrame();
          } catch (IOException e) {
            throw new ServiceRuntimeException(e);
          }
          return ret;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (IOException e) {
      throw new ServiceRuntimeException(e);
    }
  }

}
