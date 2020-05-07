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
package org.apache.reef.vortex.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * The one and only serializer for the Vortex protocol.
 */
@Private
@Unstable
public final class KryoUtils {
  /**
   * For reducing Kryo object instantiation cost.
   */
  private final KryoPool kryoPool;

  @Inject
  private KryoUtils() {
    final KryoFactory factory = new KryoFactory() {
      @Override
      public Kryo create() {
        final Kryo kryo = new Kryo();
        UnmodifiableCollectionsSerializer.registerSerializers(kryo); // Required to serialize/deserialize Throwable
        return kryo;
      }
    };
    kryoPool = new KryoPool.Builder(factory).softReferences().build();
  }

  public byte[] serialize(final Object object) {
    try (Output out = new Output(new ByteArrayOutputStream())) {
      final Kryo kryo = kryoPool.borrow();
      kryo.writeClassAndObject(out, object);
      kryoPool.release(kryo);
      return out.toBytes();
    }
  }

  public Object deserialize(final byte[] bytes) {
    try (Input input = new Input(new ByteArrayInputStream(bytes))) {
      final Kryo kryo = kryoPool.borrow();
      final Object object = kryo.readClassAndObject(input);
      kryoPool.release(kryo);
      return object;
    }
  }
}
