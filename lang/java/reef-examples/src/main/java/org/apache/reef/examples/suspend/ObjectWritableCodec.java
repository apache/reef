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
package org.apache.reef.examples.suspend;

import org.apache.hadoop.io.Writable;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Codec for Hadoop Writable object serialization.
 *
 * @param <T> Class derived from Hadoop Writable.
 */
public class ObjectWritableCodec<T extends Writable> implements Codec<T> {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(ObjectWritableCodec.class.getName());

  /**
   * we need it to invoke the class constructor.
   */
  private final Class<? extends T> writableClass;

  /**
   * Create a new codec for Hadoop Writables.
   *
   * @param clazz we need it to invoke the class constructor.
   */
  public ObjectWritableCodec(final Class<? extends T> clazz) {
    this.writableClass = clazz;
  }

  /**
   * Encodes Hadoop Writable object into a byte array.
   *
   * @param writable the object to encode.
   * @return serialized object as byte array.
   * @throws RemoteRuntimeException if serialization fails.
   */
  @Override
  public byte[] encode(final T writable) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(bos)) {
      writable.write(dos);
      return bos.toByteArray();
    } catch (final IOException ex) {
      LOG.log(Level.SEVERE, "Cannot encode object " + writable, ex);
      throw new RemoteRuntimeException(ex);
    }
  }

  /**
   * Decode Hadoop Writable object from a byte array.
   *
   * @param buffer serialized version of the Writable object (as a byte array).
   * @return a Writable object.
   * @throws RemoteRuntimeException if deserialization fails.
   */
  @Override
  public T decode(final byte[] buffer) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
         DataInputStream dis = new DataInputStream(bis)) {
      final T writable = this.writableClass.newInstance();
      writable.readFields(dis);
      return writable;
    } catch (final IOException | InstantiationException | IllegalAccessException ex) {
      LOG.log(Level.SEVERE, "Cannot decode class " + this.writableClass, ex);
      throw new RemoteRuntimeException(ex);
    }
  }
}
