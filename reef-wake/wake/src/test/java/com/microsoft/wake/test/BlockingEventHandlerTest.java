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
package com.microsoft.wake.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.BlockingEventHandler;

public class BlockingEventHandlerTest {

  @Test
  public void testSingle() {
    final AtomicInteger i = new AtomicInteger(0);
    final AtomicInteger cnt = new AtomicInteger(0);

    BlockingEventHandler<Integer> h = new BlockingEventHandler<>(1, new EventHandler<Iterable<Integer>>() {
      @Override
      public void onNext(Iterable<Integer> value) {
        for (int x : value) {
          i.getAndAdd(x);
          cnt.incrementAndGet();
        }
      }
    });
    h.onNext(0xBEEF);
    assertEquals(0xBEEF, i.get());
    assertEquals(1, cnt.get());
  }

  @Test
  public void testMultiple() {
    final AtomicInteger i = new AtomicInteger(0);
    final AtomicInteger cnt = new AtomicInteger(0);

    BlockingEventHandler<Integer> h = new BlockingEventHandler<>(1, new EventHandler<Iterable<Integer>>() {
      @Override
      public void onNext(Iterable<Integer> value) {
        for (int x : value) {
          i.getAndAdd(x);
          cnt.incrementAndGet();
        }
      }
    });
    h.onNext(0xBEEF);
    h.onNext(0xDEAD);
    h.onNext(0xC0FFEE);
    assertEquals(0xBEEF + 0xDEAD + 0xC0FFEE, i.get());
    assertEquals(3, cnt.get());
  }

  @Test
  public void testTwoStreams() {
    final AtomicInteger i = new AtomicInteger(0);
    final AtomicInteger cnt = new AtomicInteger(0);

    final int num = 1000;
    final BlockingEventHandler<Integer> h = new BlockingEventHandler<>(2 * num, new EventHandler<Iterable<Integer>>() {
      @Override
      public void onNext(Iterable<Integer> value) {
        for (int x : value) {
          i.getAndAdd(x);
          cnt.incrementAndGet();
        }
      }
    });

    Runnable r = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < num; i++) {
          h.onNext(i);
        }
      }
    };
    Thread a = new Thread(r);
    Thread b = new Thread(r);
    a.start();
    b.start();
    try {
      a.join();
      b.join();
    } catch (InterruptedException e) {
      fail(e.toString());
    }

    assertEquals(2 * (num - 1) * num / 2, i.get());
    assertEquals(2 * num, cnt.get());
  }

  @Test
  public void testMultipleResets() {
    final AtomicInteger i = new AtomicInteger(0);
    final AtomicInteger cnt = new AtomicInteger(0);
    final AtomicInteger cntcall = new AtomicInteger(0);

    final int num = 1000;
    final int events = 10;
    final BlockingEventHandler<Integer> h = new BlockingEventHandler<>(2 * num, new EventHandler<Iterable<Integer>>() {
      @Override
      public void onNext(Iterable<Integer> value) {
        for (int x : value) {
          i.getAndAdd(x);
          cnt.incrementAndGet();
        }
        cntcall.incrementAndGet();
      }
    });

    Runnable r = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < num*events; i++) {
          h.onNext(i);
        }
      }
    };
    Thread a = new Thread(r);
    Thread b = new Thread(r);
    a.start();
    b.start();
    try {
      a.join();
      b.join();
    } catch (InterruptedException e) {
      fail(e.toString());
    }

    assertEquals(2 * (num*events - 1) * (num*events) / 2, i.get());
    assertEquals(2 * num * events, cnt.get());
    assertEquals(events, cntcall.get());
  }
  
  
  @Test
  public void testManyStreams() {
    final AtomicInteger i = new AtomicInteger(0);
    final AtomicInteger cnt = new AtomicInteger(0);

    final int num = 1000;
    final BlockingEventHandler<Integer> h = new BlockingEventHandler<>(num, new EventHandler<Iterable<Integer>>() {
      @Override
      public void onNext(Iterable<Integer> value) {
        for (int x : value) {
          i.getAndAdd(x);
          cnt.incrementAndGet();
        }
      }
    });

    final int val = 7;
    Runnable r = new Runnable() {
      @Override
      public void run() {
        h.onNext(val);
      }
    };

    Thread[] workers = new Thread[num];
    for (int ii = 0; ii < workers.length; ii++)
      workers[ii] = new Thread(r);
    for (Thread w : workers)
      w.start();
    try {
      for (Thread w : workers)
        w.join();
    } catch (InterruptedException e) {
      fail(e.toString());
    }

    assertEquals(val * num, i.get());
    assertEquals(num, cnt.get());

  }

}
