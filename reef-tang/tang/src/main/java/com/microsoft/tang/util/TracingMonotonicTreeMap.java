/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.util;

import com.microsoft.tang.BindLocation;
import com.microsoft.tang.implementation.StackBindLocation;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class TracingMonotonicTreeMap<K, V> implements TracingMonotonicMap<K, V> {
  private class EntryImpl implements Map.Entry<V, BindLocation> {
    private final V key;
    private final BindLocation value;

    EntryImpl(V key, BindLocation value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public V getKey() {
      return key;
    }

    @Override
    public BindLocation getValue() {
      return value;
    }

    @Override
    public BindLocation setValue(BindLocation value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "[" + key + "] set by " + value;
    }

  }

  private final MonotonicTreeMap<K, EntryImpl> innerMap = new MonotonicTreeMap<>();


  @Override
  public void clear() {
    innerMap.clear();
  }

  @Override
  public boolean containsKey(Object key) {
    return innerMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object key) {
    EntryImpl ret = innerMap.get(key);
    return ret != null ? ret.getKey() : null;
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public Set<K> keySet() {
    return innerMap.keySet();
  }

  @Override
  public V put(K key, V value) {
    EntryImpl ret = innerMap.put(key, new EntryImpl(value, new StackBindLocation()));
    return ret != null ? ret.getKey() : null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public Collection<V> values() {
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

    final TracingMonotonicTreeMap that = (TracingMonotonicTreeMap) o;

    if (innerMap != null ? !innerMap.equals(that.innerMap) : that.innerMap != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return innerMap != null ? innerMap.hashCode() : 0;
  }
}
