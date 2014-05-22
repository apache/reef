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
package com.microsoft.reef.io.storage.util;

import com.microsoft.reef.io.Tuple;

import java.util.Comparator;

public final class TupleKeyComparator<K,V> implements
    Comparator<Tuple<K, V>> {
  private final Comparator<K> c;

  public TupleKeyComparator(Comparator<K> c) {
    this.c = c;
  }

  @Override
  public int compare(Tuple<K,V> o1, Tuple<K,V> o2) {
    return c.compare(o1.getKey(), o2.getKey());
  }
}