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
package org.apache.reef.util.cache;

import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map.Entry;

/**
 * Implementation that supports expire-after-write.
 * Entries that have expired are collected and invalidated on get calls.
 * This obviates the need for a separate thread to invalidate expired entries, at
 * the cost of some increase in get call latency.
 * The invalidation sweep is only initiated after an interval (expireCheckInterval)
 * has passed, and at most one invalidation sweep is run at a time.
 *
 * Operations on a single key are linearizable. The argument is:
 * 1. The putIfAbsent call in get guarantees that loadAndGet is called exactly once
 *    for a WrappedValue instance that is put into the map: All putIfAbsent calls
 *    that return the WrappedValue instance will return the value loaded by loadAndGet.
 * 2. Concurrent putIfAbsent and remove calls on a key have an ordering: if putIfAbsent
 *    returns null then it happened after the remove (and a new value will be loaded);
 *    else if it returns non-null then it happened before the remove
 *    (and the previous value will be returned).
 */
public final class CacheImpl<K, V> implements Cache<K, V> {
  private final ConcurrentMap<K, WrappedValue<V>> internalMap;
  private final CurrentTime currentTime;
  private final long timeoutMillis;
  private final long expireCheckInterval;
  private final AtomicBoolean expireInProgress;

  private long expireCheckedTime;

  /**
   * Construct an expire-after-write cache.
   *
   * @param currentTime   class that returns the current time for timeout purposes
   * @param timeoutMillis a cache entry timeout after write
   */
  @Inject
  public CacheImpl(final CurrentTime currentTime,
                   final long timeoutMillis) {
    this.internalMap = new ConcurrentHashMap<>();
    this.currentTime = currentTime;
    this.timeoutMillis = timeoutMillis;
    this.expireCheckInterval = timeoutMillis / 2;
    this.expireInProgress = new AtomicBoolean(false);

    this.expireCheckedTime = currentTime.now();
  }

  @Override
  public V get(final K key, final Callable<V> valueFetcher) throws ExecutionException {
    // Before get, try to invalidate as many expired as possible
    expireEntries();

    final WrappedValue<V> newWrappedValue = new WrappedValue<>(valueFetcher, currentTime);
    final WrappedValue<V> existingWrappedValue = internalMap.putIfAbsent(key, newWrappedValue);

    if (existingWrappedValue == null) {
      // If absent, compute and return
      return newWrappedValue.loadAndGet();
    } else {
      final Optional<V> existingValue = existingWrappedValue.getValue();
      if (existingValue.isPresent()) {
        // If value already exists, get (without locking) and return
        return existingValue.get();
      } else {
        // If value is being computed, wait for computation to complete
        return existingWrappedValue.waitAndGet();
      }
    }
  }

  private void expireEntries() {
    if (expireInProgress.compareAndSet(false, true)) {
      final long now = currentTime.now();
      if (expireCheckedTime + expireCheckInterval < now) {
        expireEntriesAtTime(now);
        expireCheckedTime = now;
      }
      expireInProgress.compareAndSet(true, false);
    }
  }

  private void expireEntriesAtTime(final long now) {
    for (final Entry<K, WrappedValue<V>> entry : internalMap.entrySet()) {
      if (entry.getValue() != null) {
        final Optional<Long> writeTime = entry.getValue().getWriteTime();
        if (writeTime.isPresent() && writeTime.get() + timeoutMillis < now) {
          invalidate(entry.getKey());
        }
      }
    }
  }

  @Override
  public void invalidate(final K key) {
    internalMap.remove(key);
  }
}
