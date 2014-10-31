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
import org.apache.reef.io.storage.util.GetAllIterable;

import javax.inject.Inject;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Simple in-memory ExternalMap implementation.  This class does not require
 * any codecs, and so is guaranteed to be instantiable.  Therefore, it is the
 * default ExternalMap provided by StorageManagerRam.
 */
public class RamMap<T> implements ExternalMap<T> {
  private final ConcurrentSkipListMap<CharSequence, T> map
      = new ConcurrentSkipListMap<CharSequence, T>();

  @Inject
  public RamMap(RamStorageService ramStore) {
    //this.localStore = localStore;
  }

  @Override
  public boolean containsKey(CharSequence key) {
    return map.containsKey(key);
  }

  @Override
  public T get(CharSequence key) {
    return map.get(key);
  }

  @Override
  public T put(CharSequence key, T value) {
    return map.put(key, value);
  }

  @Override
  public T remove(CharSequence key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends CharSequence, ? extends T> m) {
    map.putAll(m);
  }

  @Override
  public Iterable<Entry<CharSequence, T>> getAll(Set<? extends CharSequence> keys) {
    return new GetAllIterable<>(keys, this);
  }

}
