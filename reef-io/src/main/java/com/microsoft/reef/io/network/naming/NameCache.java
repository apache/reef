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
package com.microsoft.reef.io.network.naming;

import com.google.common.cache.CacheBuilder;
import com.microsoft.reef.io.network.Cache;
import com.microsoft.wake.Identifier;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Naming cache implementation
 */
public class NameCache implements Cache<Identifier, InetSocketAddress> {

  private final com.google.common.cache.Cache<Identifier, InetSocketAddress> cache;

  /**
   * Constructs a naming cache
   *
   * @param timeout a cache entry timeout after access
   */
  public NameCache(long timeout) {
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(timeout, TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Gets an address for an identifier
   *
   * @param key          an identifier
   * @param valueFetcher a callable to load a value for the corresponding identifier
   * @return an Internet socket address
   * @throws ExecutionException
   */
  @Override
  public InetSocketAddress get(Identifier key,
                               Callable<InetSocketAddress> valueFetcher) throws ExecutionException {
    return cache.get(key, valueFetcher);
  }

  /**
   * Invalidates the entry for an identifier
   *
   * @param key an identifier
   */
  @Override
  public void invalidate(Identifier key) {
    cache.invalidate(key);
  }

}
