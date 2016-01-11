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
package org.apache.reef.wake.examples.accumulate;


import org.apache.reef.wake.Stage;
import org.apache.reef.wake.rx.Observer;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * key-value pair Combiner stage.
 *
 * @param <K> key
 * @param <V> value
 */
public class CombinerStage<K extends Comparable<K>, V> implements Stage {

  private final Combiner<K, V> c;
  private final Observer<Map.Entry<K, V>> o;
  private final OutputThread worker = new OutputThread();
  private final ConcurrentSkipListMap<K, V> register = new ConcurrentSkipListMap<>();
  private volatile boolean done = false;

  public CombinerStage(final Combiner<K, V> c, final Observer<Map.Entry<K, V>> o) {
    this.c = c;
    this.o = o;
    worker.start();
  }

  public Observer<Map.Entry<K, V>> wireIn() {
    return new Observer<Map.Entry<K, V>>() {
      @Override
      public void onNext(final Map.Entry<K, V> pair) {
        V old;
        V newVal;
        final boolean wasEmpty = register.isEmpty();
        boolean succ = false;

        while (!succ) {
          old = register.get(pair.getKey());
          newVal = c.combine(pair.getKey(), old, pair.getValue());
          if (old == null) {
            succ = null == register.putIfAbsent(pair.getKey(), newVal);
          } else {
            succ = register.replace(pair.getKey(), old, newVal);
          }
        }

        if (wasEmpty) {
          synchronized (register) {
            register.notify();
          }
        }
      }

      @Override
      public void onError(final Exception error) {
        o.onError(error);
      }

      @Override
      public void onCompleted() {
        synchronized (register) {
          done = true;
          if (register.isEmpty()) {
            register.notify();
          }
        }
      }
    };
  }

  @Override
  public void close() throws Exception {
    worker.join();
  }

  /**
   * key-value pair Combiner Interface.
   *
   * @param <K> key
   * @param <V> value
   */
  public interface Combiner<K extends Comparable<K>, V> {
    V combine(K key, V old, V cur);
  }

  /**
   * A comparable key-value pair.
   *
   * @param <K> key
   * @param <V> value
   */
  public static class Pair<K extends Comparable<K>, V> implements Map.Entry<K, V>, Comparable<Map.Entry<K, V>> {
    private final K k;
    private final V v;

    public Pair(final K k, final V v) {
      this.k = k;
      this.v = v;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Pair<K, V> pair = (Pair<K, V>) o;
      return k.compareTo(pair.getKey()) == 0;
    }

    @Override
    public int hashCode() {
      return k.hashCode();
    }

    @Override
    public int compareTo(final Map.Entry<K, V> arg0) {
      return k.compareTo(arg0.getKey());
    }

    @Override
    public K getKey() {
      return k;
    }

    @Override
    public V getValue() {
      return v;
    }

    @Override
    public V setValue(final V value) {
      throw new UnsupportedOperationException();
    }
  }

  private class OutputThread extends Thread {
    OutputThread() {
      super("grouper-output-thread");
    }

    @Override
    public void run() {
      while (true) {
        if (register.isEmpty()) {
          synchronized (register) {
            while (register.isEmpty() && !done) {
              try {
                register.wait();
              } catch (final InterruptedException e) {
                throw new IllegalStateException(e);
              }
            }
            if (done) {
              break;
            }
          }
        }
        Map.Entry<K, V> cursor = register.pollFirstEntry();
        while (cursor != null) {
          o.onNext(cursor);
          final K nextKey = register.higherKey(cursor.getKey());

          /* If there is more than one OutputThread worker then the remove() -> null case
           * must be handled
           */
          cursor = (nextKey == null) ? null : new Pair<>(nextKey, register.remove(nextKey));
        }
      }
      o.onCompleted();
    }
  }

}
