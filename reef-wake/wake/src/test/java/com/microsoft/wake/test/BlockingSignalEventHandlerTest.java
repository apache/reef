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
import com.microsoft.wake.impl.BlockingSignalEventHandler;

public class BlockingSignalEventHandlerTest {

  @Test
  public void testSingle() {
    final AtomicInteger cnt = new AtomicInteger(0);

    BlockingSignalEventHandler<Integer> h = new BlockingSignalEventHandler<>(1, new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
        cnt.incrementAndGet();
      }
    });
    h.onNext(0xBEEF);
    assertEquals(1, cnt.get());
  }

  @Test
  public void testMultiple() {
    final AtomicInteger cnt = new AtomicInteger(0);

    BlockingSignalEventHandler<Integer> h = new BlockingSignalEventHandler<>(2, new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
          cnt.incrementAndGet();
        }
    });
    h.onNext(1);
    h.onNext(2);
    assertEquals(1, cnt.get());
  }

  @Test
  public void testTwoStreams() {
    final AtomicInteger cnt = new AtomicInteger(0);

    final int num = 1000;
    final BlockingSignalEventHandler<Integer> h = new BlockingSignalEventHandler<>(2 * num, new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
          cnt.incrementAndGet();
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

    assertEquals(1, cnt.get());
  }

  @Test
  public void testMultipleResets() {
    final AtomicInteger cnt = new AtomicInteger(0);

    final int num = 1000;
    final int events = 10;
    final BlockingSignalEventHandler<Integer> h = new BlockingSignalEventHandler<>(2 * num, new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
          cnt.incrementAndGet();
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

    assertEquals(events, cnt.get());
  }

  @Test
  public void testManyStreams() {
    final AtomicInteger cnt = new AtomicInteger(0);

    final int num = 1000;
    final BlockingSignalEventHandler<Integer> h = new BlockingSignalEventHandler<>(num, new EventHandler<Integer>() {
      @Override
      public void onNext(Integer value) {
        cnt.incrementAndGet();
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

    assertEquals(1, cnt.get());

  }

}
