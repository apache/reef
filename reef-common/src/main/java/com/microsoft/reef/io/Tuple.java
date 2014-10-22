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
package com.microsoft.reef.io;

import com.microsoft.reef.annotations.Unstable;

// TODO: Document
@Unstable
public final class Tuple<K, V> {
  private final K k;
  private final V v;

  public K getKey() {
    return k;
  }

  public V getValue() {
    return v;
  }

  public Tuple(final K k, final V v) {
    this.k = k;
    this.v = v;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Tuple tuple = (Tuple) o;

    if (!k.equals(tuple.k)) return false;
    if (!v.equals(tuple.v)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = k.hashCode();
    result = 31 * result + v.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "(" + getKey() + "," + getValue() + ")";
  }

}
