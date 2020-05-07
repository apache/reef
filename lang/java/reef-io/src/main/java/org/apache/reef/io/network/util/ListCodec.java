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
package org.apache.reef.io.network.util;

import org.apache.reef.wake.remote.Codec;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ListCodec<T> implements Codec<List<T>> {

  private static final Logger LOG = Logger.getLogger(ListCodec.class.getName());

  private final Codec<T> codec;

  public ListCodec(final Codec<T> codec) {
    super();
    this.codec = codec;
  }

  public static void main(final String[] args) {
    final List<String> arrList = Arrays.asList(
        new String[]{"One", "Two", "Three", "Four", "Five"});
    LOG.log(Level.FINEST, "Input: {0}", arrList);
    final ListCodec<String> lstCodec = new ListCodec<>(new StringCodec());
    final byte[] bytes = lstCodec.encode(arrList);
    LOG.log(Level.FINEST, "Output: {0}", lstCodec.decode(bytes));
  }

  @Override
  public byte[] encode(final List<T> list) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream daos = new DataOutputStream(baos)) {
      for (final T t : list) {
        final byte[] tBytes = this.codec.encode(t);
        daos.writeInt(tBytes.length);
        daos.write(tBytes);
      }
      return baos.toByteArray();
    } catch (final IOException ex) {
      LOG.log(Level.WARNING, "Error in list encoding", ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public List<T> decode(final byte[] list) {
    final List<T> result = new ArrayList<>();
    try (DataInputStream dais = new DataInputStream(new ByteArrayInputStream(list))) {
      while (dais.available() > 0) {
        final int length = dais.readInt();
        final byte[] tBytes = new byte[length];
        dais.readFully(tBytes);
        final T t = this.codec.decode(tBytes);
        result.add(t);
      }
      return result;
    } catch (final IOException ex) {
      LOG.log(Level.WARNING, "Error in list decoding", ex);
      throw new RuntimeException(ex);
    }
  }
}
