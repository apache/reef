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
package org.apache.reef.wake.test.rx;

import org.apache.reef.wake.rx.Observer;
import org.apache.reef.wake.rx.Subject;
import org.apache.reef.wake.rx.impl.TimeoutSubject;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TimeoutSubjectTest {

  @Test
  public void testSuccess() {
    final AtomicInteger nexts = new AtomicInteger(0);
    final AtomicInteger completes = new AtomicInteger(0);
    final int delta = 400;
    Subject<Integer, Integer> dut = new TimeoutSubject<>(10000, new Observer<Integer>() {

      @Override
      public void onNext(Integer value) {
        nexts.addAndGet(delta);
      }

      @Override
      public void onError(Exception error) {
        fail(error.toString());
      }

      @Override
      public void onCompleted() {
        assertEquals(delta, nexts.get());
        completes.incrementAndGet();
      }
    });
    dut.onNext(delta);

    assertEquals(delta, nexts.get());
    assertEquals(1, completes.get());
  }

  @Test
  public void testDifferentThread() {
    final AtomicInteger nexts = new AtomicInteger(0);
    final AtomicInteger completes = new AtomicInteger(0);
    final int delta = 400;
    final Subject<Integer, Integer> dut = new TimeoutSubject<>(10000, new Observer<Integer>() {

      @Override
      public void onNext(Integer value) {
        nexts.addAndGet(delta);
      }

      @Override
      public void onError(Exception error) {
        fail(error.toString());
      }

      @Override
      public void onCompleted() {
        assertEquals(delta, nexts.get());
        completes.incrementAndGet();
      }
    });

    ExecutorService e = Executors.newSingleThreadExecutor();
    e.submit(new Runnable() {
      @Override
      public void run() {
        dut.onNext(delta);
      }
    });

    e.shutdown();
    try {
      e.awaitTermination(11000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      e1.printStackTrace();
      fail(e1.toString());
    }

    assertEquals(delta, nexts.get());
    assertEquals(1, completes.get());
  }

  @Test
  public void testTimeout() {
    final int timeout = 1;
    final int sleep = 500;
    final AtomicInteger errors = new AtomicInteger(0);
    Subject<Integer, Integer> dut = new TimeoutSubject<>(timeout, new Observer<Integer>() {

      @Override
      public void onNext(Integer value) {
        fail("Should not get called");
      }

      @Override
      public void onError(Exception error) {
        assertTrue(error instanceof TimeoutException);
        errors.incrementAndGet();
      }

      @Override
      public void onCompleted() {
        fail("Should not get called");
      }
    });

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail(e.toString());
    }
    dut.onNext(0xC0FFEE);

    assertEquals(1, errors.get());
  }
}
