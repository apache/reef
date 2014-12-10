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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public final class CacheImplConcurrentTest {

  private Cache<String, Integer> cache;
  private final CurrentTime currentTime = new SystemTime();
  private final long timeoutMillis = 4000;
  private final long computationMillis = 2000;
  private final int numConcurrentCalls = 10;

  @Before
  public void setUp() {
    cache = new CacheImpl<>(currentTime, timeoutMillis);
  }

  @Test
  public void testGetReturnsFirstValue() throws ExecutionException, InterruptedException {
    final String key = "testGetReturnsFirstValue";
    final int firstValue = 20;
    final int secondValue = 40;

    final ExecutorService es = Executors.newFixedThreadPool(numConcurrentCalls);
    es.submit(new Runnable() {
      @Override
      public void run() {
        final int getFirstValue1;
        try {
          // Assert that firstValue is returned, even when other gets are called during the Callable execution
          getFirstValue1 = cache.get(key, new SleepingInteger(firstValue, computationMillis));
          assertEquals(firstValue, getFirstValue1);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    });

    Thread.sleep(500);

    for (int i = 1; i < numConcurrentCalls; i++) {
      final int index = i;
      es.submit(new Runnable() {
        @Override
        public void run() {
          try {
            // The original cached value should be retrieved
            final int getFirstValue2 = cache.get(key, new ImmediateInteger(secondValue));
            assertEquals(firstValue, getFirstValue2);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    es.shutdown();
    assertTrue(es.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetReturnsSameValue() throws InterruptedException {
    final String key = "testGetReturnsSameValue";
    final int[] values = new int[numConcurrentCalls];
    final int[] getValues = new int[numConcurrentCalls];
    for (int i = 0; i < numConcurrentCalls; i++) {
      values[i] = i;
      getValues[i] = -1;
    }

    final ExecutorService es = Executors.newFixedThreadPool(numConcurrentCalls);

    for (int i = 0; i < numConcurrentCalls; i++) {
      final int index = i;
      es.submit(new Runnable() {
        @Override
        public void run() {
          try {
            getValues[index] = cache.get(key, new ImmediateInteger(values[index]));
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    es.shutdown();
    assertTrue(es.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS));

    assertNotEquals(-1, getValues[0]);
    for (int i = 1; i < numConcurrentCalls; i++) {
      assertEquals(getValues[i-1], getValues[i]);
    }
  }

  @Test
  public void testInvalidateDuringCallableExecution() throws ExecutionException, InterruptedException {    final String key = "testGet";
    final int firstValue = 20;
    final int secondValue = 40;

    final ExecutorService es = Executors.newSingleThreadExecutor();
    es.submit(new Runnable() {
      @Override
      public void run() {
        final int getFirstValue1;
        try {
          // Assert that firstValue is returned, even when it is invalidated during the Callable execution
          getFirstValue1 = cache.get(key, new SleepingInteger(firstValue, computationMillis));
          assertEquals(firstValue, getFirstValue1);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    });

    Thread.sleep(500);

    final int indexToInvalidateOn = numConcurrentCalls / 2;
    for (int i = 1; i < numConcurrentCalls; i++) {
      final int index = i;
      if (index == indexToInvalidateOn) {
        cache.invalidate(key);
      } else if (index < indexToInvalidateOn) {
        try {
          // The original cached value should be retrieved, even when it is invalidated during the Callable execution
          final int getFirstValue2 = cache.get(key, new ImmediateInteger(secondValue));
          assertEquals(firstValue, getFirstValue2);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      } else {
        try {
          // The second value should be retrieved, because the cache has been invalidated
          final int getFirstValue2 = cache.get(key, new ImmediateInteger(secondValue));
          assertEquals(secondValue, getFirstValue2);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }

    es.shutdown();
    assertTrue(es.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testExpireOnGetSameKey() throws ExecutionException, InterruptedException {
    final String key = "testExpireOnGetSameKey";
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

  @Test
  public void testExpireOnGetDifferentKey() throws ExecutionException, InterruptedException {
    final String key = "testExpireOnGetDifferentKey";
    final String differentKey = "differentKey";
    final int firstValue = 20;
    final int secondValue = 40;

    final int getFirstValue = cache.get(key, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getFirstValue);

    // Sleep enough to trigger timeout
    Thread.sleep(timeoutMillis + timeoutMillis/2);

    final int getDifferentValue = cache.get(differentKey, new ImmediateInteger(firstValue));
    assertEquals(firstValue, getDifferentValue);

    // The next cached value should be retrieved after timeout
    final int getSecondValue = cache.get(key, new ImmediateInteger(secondValue));
    assertEquals(secondValue, getSecondValue);
  }
}
