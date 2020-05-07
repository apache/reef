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
package org.apache.reef.io.serialization;

import javax.inject.Inject;
import java.io.*;
import java.util.logging.Logger;

/**
 * A {@link Codec} for {@link Serializable} objects.
 * <p>
 * It uses java serialization, use with caution.
 *
 * @param <T> The type of objects Serialized
 */
public class SerializableCodec<T extends Serializable> implements Codec<T> {

  private static final Logger LOG = Logger.getLogger(SerializableCodec.class.getName());

  /**
   * Default constructor for TANG use.
   */
  @Inject
  public SerializableCodec() {
  }

  @Override
  public byte[] encode(final T obj) {
    try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
      try (ObjectOutputStream out = new ObjectOutputStream(bout)) {
        out.writeObject(obj);
      }
      return bout.toByteArray();
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to encode: " + obj, ex);
    }
  }

  @Override
  public T decode(final byte[] buf) {
    try {
      try (ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(buf))) {
        final T result = (T) oin.readObject();
        return result;
      }
    } catch (final IOException | ClassNotFoundException ex) {
      throw new RuntimeException("Unable to decode.", ex);
    }

  }
}
