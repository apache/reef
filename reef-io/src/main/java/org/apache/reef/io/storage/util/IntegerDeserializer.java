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

public class IntegerDeserializer implements
    Deserializer<Integer, InputStream> {
  @Override
  public Iterable<Integer> create(InputStream arg) {
    final DataInputStream dis = new DataInputStream(arg);
    return new Iterable<Integer>() {

      @Override
      public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Integer next() {
            try {
              return dis.readInt();
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