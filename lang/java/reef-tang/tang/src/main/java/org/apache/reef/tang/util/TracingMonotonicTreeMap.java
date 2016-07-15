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
package org.apache.reef.tang.util;

import org.apache.reef.tang.BindLocation;
import org.apache.reef.tang.implementation.StackBindLocation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class TracingMonotonicTreeMap<K, V> implements TracingMonotonicMap<K, V> {
  private final MonotonicTreeMap<K, EntryImpl> innerMap = new MonotonicTreeMap<>();

  @Override
  public void clear() {
    innerMap.clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    return innerMap.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    // This implementation is not efficient, but we need to use entrySet() to pass findbugs' static code analysis
    final Set<Map.Entry<K, EntryImpl>> innerMapEntrySet = innerMap.entrySet();
    final Set<Map.Entry<K, V>> entrySet = new HashSet<>();
    for(final Map.Entry<K, EntryImpl> entry: innerMapEntrySet) {
      entrySet.add(new Entry(entry.getKey(), entry.getValue().getKey()));
    }
    return entrySet;
  }

  @Override
  public V get(final Object key) {
    final EntryImpl ret = innerMap.get(key);
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
  public V put(final K key, final V value) {
    final EntryImpl ret = innerMap.put(key, new EntryImpl(value, new StackBindLocation()));
    return ret != null ? ret.getKey() : null;
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(final Object key) {
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
  public boolean equals(final Object o) {
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

  private class EntryImpl implements Map.Entry<V, BindLocation> {
    private final V key;
    private final BindLocation value;

    EntryImpl(final V key, final BindLocation value) {
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
    @SuppressWarnings("checkstyle:hiddenfield")
    public BindLocation setValue(final BindLocation value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "[" + key + "] set by " + value;
    }
  }

  /**
   * An implementation class for representing TracingMonotoricTreeMap's entry.
   */
  private class Entry implements Map.Entry<K, V> {
    private final K key;
    private final V value;

    Entry(final K key, final V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(final V newValue) {
      throw new UnsupportedOperationException();
    }
  }

}
