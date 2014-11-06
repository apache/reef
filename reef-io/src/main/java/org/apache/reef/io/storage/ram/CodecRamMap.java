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
package org.apache.reef.io.storage.ram;

import org.apache.reef.io.ExternalMap;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.storage.util.GetAllIterable;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class CodecRamMap<T> implements ExternalMap<T> {

  private final Codec<T> c;
  private final ConcurrentSkipListMap<CharSequence, byte[]> map;

  @Inject
  public CodecRamMap(RamStorageService ramStore,
                     @Parameter(RamMapCodec.class) final Codec<T> c) {
    this.c = c;
    this.map = new ConcurrentSkipListMap<CharSequence, byte[]>();
  }

  @Override
  public boolean containsKey(final CharSequence key) {
    return map.containsKey(key);
  }

  @Override
  public T get(final CharSequence key) {
    final byte[] ret = map.get(key);
    return ret != null ? c.decode(ret) : null;
  }

  @Override
  public T put(final CharSequence key, T value) {
    final byte[] ret = map.put(key, c.encode(value));
    return ret != null ? c.decode(ret) : null;
  }

  @Override
  public T remove(final CharSequence key) {
    final byte[] ret = map.remove(key);
    return ret != null ? c.decode(ret) : null;
  }

  @Override
  public void putAll(final Map<? extends CharSequence, ? extends T> m) {
    for (final CharSequence x : m.keySet()) {
      map.put(x, c.encode(m.get(x)));
    }
  }

  @Override
  public Iterable<Map.Entry<CharSequence, T>> getAll(
      final Set<? extends CharSequence> keys) {
    return new GetAllIterable<T>(keys, this);
  }

  @NamedParameter
  static public class RamMapCodec implements Name<Codec<?>> {
  }
}
