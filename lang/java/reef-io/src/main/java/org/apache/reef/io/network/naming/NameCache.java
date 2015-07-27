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
package org.apache.reef.io.network.naming;

import org.apache.reef.util.cache.Cache;
import org.apache.reef.util.cache.CacheImpl;
import org.apache.reef.util.cache.SystemTime;
import org.apache.reef.wake.Identifier;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Naming cache implementation.
 */
public class NameCache implements Cache<Identifier, InetSocketAddress> {

  private final Cache<Identifier, InetSocketAddress> cache;

  /**
   * Constructs a naming cache.
   *
   * @param timeout a cache entry timeout after write
   */
  public NameCache(final long timeout) {
    cache = new CacheImpl<>(new SystemTime(), timeout);
  }

  /**
   * Gets an address for an identifier.
   *
   * @param key          an identifier
   * @param valueFetcher a callable to load a value for the corresponding identifier
   * @return an Internet socket address
   * @throws ExecutionException
   */
  @Override
  public InetSocketAddress get(final Identifier key,
                               final Callable<InetSocketAddress> valueFetcher) throws ExecutionException {
    return cache.get(key, valueFetcher);
  }

  /**
   * Invalidates the entry for an identifier.
   *
   * @param key an identifier
   */
  @Override
  public void invalidate(final Identifier key) {
    cache.invalidate(key);
  }

}
