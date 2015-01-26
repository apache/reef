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
package org.apache.reef.io.storage;

import org.apache.reef.io.Tuple;
import org.apache.reef.io.storage.util.TupleKeyComparator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MergingIterator<T> implements Iterator<T> {
  private final PriorityQueue<Tuple<T, Iterator<T>>> heap;

  public MergingIterator(final Comparator<T> c, Iterator<T>[] its) {
    this.heap = new PriorityQueue<Tuple<T, Iterator<T>>>(11,
        new TupleKeyComparator<T, Iterator<T>>(c));

    for (Iterator<T> it : its) {
      T b = it.hasNext() ? it.next() : null;
      if (b != null) {
        heap.add(new Tuple<>(b, it));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return heap.size() != 0;
  }

  @Override
  public T next() {
    Tuple<T, Iterator<T>> ret = heap.remove();
    if (ret.getValue().hasNext()) {
      heap.add(new Tuple<>(ret.getValue().next(), ret.getValue()));
    }
    return ret.getKey();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException(
        "Cannot remove entires from MergingIterator!");
  }
}
