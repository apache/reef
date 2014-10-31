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
package org.apache.reef.io.storage.util;

import org.apache.reef.exception.evaluator.ServiceRuntimeException;
import org.apache.reef.io.serialization.Deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class StringDeserializer implements
    Deserializer<String, InputStream> {
  @Override
  public Iterable<String> create(InputStream arg) {
    final DataInputStream dis = new DataInputStream(arg);
    return new Iterable<String>() {

      @Override
      public Iterator<String> iterator() {
        return new Iterator<String>() {

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public String next() {
            int len = 0;
            try {
              len = dis.readInt();
              byte[] b = new byte[len];
              dis.readFully(b);
              return new String(b);
            } catch (IOException e) {
              throw new ServiceRuntimeException(e);
            }
          }

          @Override
          public boolean hasNext() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}