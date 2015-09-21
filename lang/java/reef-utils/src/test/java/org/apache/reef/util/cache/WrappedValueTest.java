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

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * Test for WrappedValue.
 */
public final class WrappedValueTest {
  private static final CurrentTime SYSTEM_TIME = new SystemTime();
  private static final int NUM_THREADS = 10;

  @Test
  public void testLoadAndGet() throws ExecutionException {
    final Integer value = 5;
    final WrappedValue<Integer> wrappedValue = new WrappedValue<>(new ImmediateInteger(value), SYSTEM_TIME);

    assertFalse(wrappedValue.getValue().isPresent());

    final Integer loadedValue = wrappedValue.loadAndGet();
    assertEquals(value, loadedValue);
    assertEquals(value, wrappedValue.getValue().get());
    assertTrue(value == loadedValue);
  }

  @Test
  public void testWaitAndGetOnPreviouslyLoadedValue() throws ExecutionException {
    final Integer value = 5;
    final WrappedValue<Integer> wrappedValue = new WrappedValue<>(new ImmediateInteger(value), SYSTEM_TIME);
    final Integer loadedValue = wrappedValue.loadAndGet();
    final Integer waitedValue = wrappedValue.waitAndGet();

    assertEquals(value, waitedValue);
    assertTrue(value == waitedValue);

    assertEquals(loadedValue, waitedValue);
    assertTrue(loadedValue == waitedValue);
  }

  @Test
  public void testConcurrentLoadWaitAndGet() throws ExecutionException, InterruptedException {
    final Integer value = 5;
    final long sleepMillis = 2000;
    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

    final WrappedValue<Integer> wrappedValue = new WrappedValue<>(
            new SleepingInteger(value, sleepMillis), SYSTEM_TIME);
    final Integer loadedValue = wrappedValue.loadAndGet();

    final Future<?>[] futures = new Future<?>[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      futures[i] = executorService.submit(new Runnable() {
        @Override
        public void run() {
          final Integer valueAfterWait = wrappedValue.waitAndGet();
          assertEquals(value, valueAfterWait);
          assertTrue(value == valueAfterWait);
        }
      });
    }
    for (int i = 0; i < NUM_THREADS; i++) {
      futures[i].get();
    }

    assertEquals(value, loadedValue);
    assertTrue(value == wrappedValue.getValue().get());
    assertTrue(value == loadedValue);
  }
}
