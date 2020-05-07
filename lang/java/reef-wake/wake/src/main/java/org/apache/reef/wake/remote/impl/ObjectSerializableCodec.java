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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import javax.inject.Inject;
import java.io.*;

/**
 * Codec that uses Java serialization.
 *
 * @param <T> type
 */
public class ObjectSerializableCodec<T> implements Codec<T> {

  @Inject
  public ObjectSerializableCodec() {
  }

  /**
   * Encodes the object to bytes.
   *
   * @param obj the object
   * @return bytes
   * @throws RemoteRuntimeException
   */
  @Override
  public byte[] encode(final T obj) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(obj);
      return bos.toByteArray();
    } catch (final IOException ex) {
      throw new RemoteRuntimeException(ex);
    }
  }

  /**
   * Decodes an object from the bytes.
   *
   * @param buf the bytes
   * @return an object
   * @throws RemoteRuntimeException
   */
  @SuppressWarnings("unchecked")
  @Override
  public T decode(final byte[] buf) {
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buf))) {
      return (T) in.readObject();
    } catch (final ClassNotFoundException | IOException ex) {
      throw new RemoteRuntimeException(ex);
    }
  }
}
