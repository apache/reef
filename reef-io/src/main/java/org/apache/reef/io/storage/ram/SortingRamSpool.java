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

import org.apache.reef.exception.evaluator.StorageException;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.Spool;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class SortingRamSpool<T> implements Spool<T> {
  private final PriorityQueue<T> heap;
  private boolean ready = false;
  private Accumulator<T> acc = new Accumulator<T>() {
    @Override
    public void add(T datum) throws StorageException {
      if (ready)
        throw new IllegalStateException("add called after close!");
      heap.add(datum);
    }

    @Override
    public void close() throws StorageException {
      ready = true;
    }
  };
  private Iterator<T> it = new Iterator<T>() {

    @Override
    public boolean hasNext() {
      return !heap.isEmpty();
    }

    @Override
    public T next() {
      return heap.remove();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "This iterator consumes the data it returns. remove() does not make any sense!");
    }

  };

  public SortingRamSpool() {
    heap = new PriorityQueue<>();
  }

  public SortingRamSpool(Comparator<T> c) {
    heap = new PriorityQueue<>(11, c);
  }

  @Override
  public Iterator<T> iterator() {
    if (!ready) {
      throw new IllegalStateException("Cannot call iterator() while accumulator is still open!");
    }
    Iterator<T> ret = it;
    it = null;
    return ret;
  }

  @Override
  public Accumulator<T> accumulator() throws StorageException {
    Accumulator<T> ret = acc;
    acc = null;
    return ret;
  }

}
