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
package org.apache.reef.util.cache;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public final class CacheImplTest {

  private Cache<String, Integer> cache;
  private final CurrentTime currentTime = new SystemTime();
  private final long timeoutMillis = 3000;

  @Before
  public void setUp() {
    cache = new CacheImpl<>(currentTime, timeoutMillis);
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    final String key = "testGet";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue1 = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue1);

    // The original cached value should be retrieved before timeout
    final int getFirstValue2 = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(firstValue, getFirstValue2);

  }

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

  @Test
  public void testExpire() throws ExecutionException, InterruptedException {
    final String key = "testGet";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue);

    // Sleep enough to trigger timeout
    Thread.sleep(timeoutMillis + timeoutMillis/2);

    // The next cached value should be retrieved after timeout
    final int getSecondValue = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(secondValue, getSecondValue);
  }
}
