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
package org.apache.reef.tang.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class MonotonicTreeMap<T, U> implements Map<T, U> {
  private static final long serialVersionUID = 1L;

  private final TreeMap<T, U> innerMap = new TreeMap<>();


  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public boolean containsKey(final Object o) {
    return innerMap.containsKey(o);
  }

  @Override
  public boolean containsValue(final Object o) {
    return innerMap.containsValue(o);
  }

  @Override
  public U get(final Object o) {
    return innerMap.get(o);
  }

  @Override
  public U put(T key, U value) {
    U old = innerMap.get(key);
    if (old != null) {
      throw new IllegalArgumentException("Attempt to re-add: [" + key
          + "]\n old value: " + old + " new value " + value);
    }
    return innerMap.put(key, value);
  }

  @Override
  public void putAll(Map<? extends T, ? extends U> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<T> keySet() {
    return innerMap.keySet();
  }

  @Override
  public Collection<U> values() {
    return innerMap.values();
  }

  @Override
  public Set<Entry<T, U>> entrySet() {
    return innerMap.entrySet();
  }

  @Override
  public U remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final MonotonicTreeMap that = (MonotonicTreeMap) o;

    if (!innerMap.equals(that.innerMap)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return innerMap.hashCode();
  }
}