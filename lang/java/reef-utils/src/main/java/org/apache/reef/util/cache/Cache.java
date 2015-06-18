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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Cache with get-if-absent-compute semantics.
 * Supports explicit invalidation.
 * Implementation may add other features, e.g. eviction on expire-after-write
 */
public interface Cache<K, V> {
  /**
   * Returns a value for the key if cached; otherwise creates, caches and returns.
   * When it creates a value for a key, only one callable for the key is executed
   *
   * @param key          a key
   * @param valueFetcher a value fetcher
   * @return a value
   * @throws ExecutionException
   */
  V get(K key, Callable<V> valueFetcher) throws ExecutionException;

  /**
   * Invalidates a key from the cache.
   *
   * @param key a key
   */
  void invalidate(K key);

}
