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

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * Test basic access of CacheImpl.
 */
public final class CacheImplTest {

  private Cache<String, Integer> cache;
  private final CurrentTime currentTime = new SystemTime();
  private final long timeoutMillis = 3000;

  @Before
  public void setUp() {
    cache = new CacheImpl<>(currentTime, timeoutMillis);
  }

  /**
   * Test that an immediate get on the same key returns the cached value instead of triggering a new computation.
   */
  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    final String key = "testGet";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue1 = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue1);

    // The original cached value should be retrieved if called immediately (before a timeout)
    final int getFirstValue2 = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(firstValue, getFirstValue2);

  }

  /**
   * Test that an invalidate clears the cached value, so the next access triggers a new computation.
   */
  @Test
  public void testInvalidate() throws ExecutionException {
    final String key = "testGet";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getValue = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getValue);

    cache.invalidate(key);

    // The second cached value should be retrieved after invalidation
    final int getSecondValue = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(secondValue, getSecondValue);
  }

  /**
   * Test expire-after-write by sleeping beyond the timeout.
   * Also, the test is designed to fail if the cache is actually expire-after-access.
   */
  @Test
  public void testExpireOnWrite() throws ExecutionException, InterruptedException {
    final String key = "testExpireOnWrite";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue1 = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue1);

    // Sleep less than timeout and do another access; value should be the same
    Thread.sleep(timeoutMillis/2);
    final int getFirstValue2 = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue2);

    // Sleep enough to trigger expire-after-write timeout
    Thread.sleep(timeoutMillis + timeoutMillis/4);
    // The next cached value should be retrieved after timeout
    final int getSecondValue = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(secondValue, getSecondValue);
  }

  /**
   * Test expire-after-write is implemented _per-key_.
   * The test is designed to fail if the cache actually resets the timer on a write to a different key.
   */
  @Test
  public void testExpireOnWritePerKey() throws ExecutionException, InterruptedException {
    final String key = "testExpireOnWritePerKey";
    final String differentKey = "differentKey";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue);

    // Sleep less than timeout and do a write on a different key; it should not affect
    // the expiration of the original key
    Thread.sleep(timeoutMillis/2);
    final int getFirstValueForDifferentKey = cache.get(differentKey, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValueForDifferentKey);

    // Sleep enough to trigger timeout
    Thread.sleep(timeoutMillis + timeoutMillis/4);

    // The next cached value should be retrieved after timeout
    final int getSecondValue = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(secondValue, getSecondValue);
  }
}
