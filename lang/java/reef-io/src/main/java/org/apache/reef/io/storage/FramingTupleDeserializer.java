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

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.exception.evaluator.ServiceRuntimeException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.serialization.Deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FramingTupleDeserializer<K, V> implements
    Deserializer<Tuple<K, V>, InputStream> {

  private final Deserializer<K, InputStream> keyDeserializer;
  private final Deserializer<V, InputStream> valDeserializer;

  public FramingTupleDeserializer(Deserializer<K, InputStream> keyDeserializer,
                                  Deserializer<V, InputStream> valDeserializer) {
    this.keyDeserializer = keyDeserializer;
    this.valDeserializer = valDeserializer;
  }

  @Override
  public Iterable<Tuple<K, V>> create(InputStream ins) {
    final DataInputStream in = new DataInputStream(ins);
    final Iterable<K> keyItbl = keyDeserializer.create(in);
    final Iterable<V> valItbl = valDeserializer.create(in);
    return new Iterable<Tuple<K, V>>() {
      @Override
      public Iterator<Tuple<K, V>> iterator() {
        final Iterator<K> keyIt = keyItbl.iterator();
        final Iterator<V> valIt = valItbl.iterator();
        try {
          return new Iterator<Tuple<K, V>>() {

            private int readFrameLength() throws ServiceException {
              try {
                return in.readInt();
              } catch (IOException e) {
                throw new ServiceRuntimeException(e);
              }
            }

            int nextFrameLength = readFrameLength();

            @Override
            public boolean hasNext() {
              return nextFrameLength != -1;
            }

            @Override
            public Tuple<K, V> next() {
              try {
                if (nextFrameLength == -1) {
                  throw new NoSuchElementException();
                }
                K k = keyIt.next();
                readFrameLength();
                V v = valIt.next();
                nextFrameLength = readFrameLength();
                return new Tuple<>(k, v);
              } catch (ServiceException e) {
                throw new ServiceRuntimeException(e);
              }
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        } catch (ServiceException e) {
          throw new ServiceRuntimeException(e);
        }
      }
    };
  }

}
