/*
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
import org.apache.reef.exception.evaluator.StorageException;
import org.apache.reef.io.Accumulable;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.serialization.Serializer;

import java.io.IOException;
import java.io.OutputStream;

public class FramingTupleSerializer<K, V> implements
    Serializer<Tuple<K, V>, OutputStream> {

  private final Serializer<K, OutputStream> keySerializer;
  private final Serializer<V, OutputStream> valSerializer;

  public FramingTupleSerializer(
      final Serializer<K, OutputStream> keySerializer,
      final Serializer<V, OutputStream> valSerializer) {
    this.keySerializer = keySerializer;
    this.valSerializer = valSerializer;
  }

  @Override
  public Accumulable<Tuple<K, V>> create(final OutputStream os) {
    final FramingOutputStream faos = new FramingOutputStream(os);

    return new Accumulable<Tuple<K, V>>() {

      @Override
      public Accumulator<Tuple<K, V>> accumulator() throws ServiceException {

        final Accumulator<K> keyAccumulator = keySerializer.create(faos)
            .accumulator();
        final Accumulator<V> valAccumulator = valSerializer.create(faos)
            .accumulator();
        return new Accumulator<Tuple<K, V>>() {
          private boolean first = true;

          @Override
          public void add(final Tuple<K, V> datum) throws ServiceException {
            try {
              if (!first) {
                faos.nextFrame();
              }
              first = false;
              keyAccumulator.add(datum.getKey());
              faos.nextFrame();
              valAccumulator.add(datum.getValue());
            } catch (final IOException e) {
              throw new ServiceException(e);
            }
          }

          @Override
          public void close() throws ServiceException {
            try {
              keyAccumulator.close();
              valAccumulator.close();
              faos.close();
            } catch (final IOException e) {
              throw new StorageException(e);
            }
          }
        };
      }
    };
  }

}
