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
package org.apache.reef.wake.test.examples;

import org.apache.reef.wake.examples.accumulate.CombinerStage;
import org.apache.reef.wake.examples.accumulate.CombinerStage.Combiner;
import org.apache.reef.wake.rx.Observer;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for CombinerStage.
 */
public class TestCombiner {

  private static final int BUCKET_COUNT = 1000;
  private static final int THREAD_COUNT = 1;
  private static final int EVENTS_COUNT = 2 * 1000000;
  private static final int EVENTS_PER_THREAD = EVENTS_COUNT / THREAD_COUNT;

  private Observer<Entry<Integer, Integer>> o;

  private volatile boolean done = false;

  @Test
  public void test() throws Exception {
    final CombinerStage<Integer, Integer> stage = new CombinerStage<>(
        new Combiner<Integer, Integer>() {

          @Override
          public Integer combine(final Integer key, final Integer old, final Integer cur) {
            if (old != null) {
              return old.intValue() + cur.intValue();
            } else {
              return cur;
            }
          }

        }, new Observer<Entry<Integer, Integer>>() {
          private AtomicInteger x = new AtomicInteger(0);

          @Override
          public void onNext(final Entry<Integer, Integer> value) {
            x.incrementAndGet();
            try {
              if (!done) {
                Thread.sleep(10);
              }
            } catch (final InterruptedException e) {
              throw new IllegalStateException(e);
            }
          }

          @Override
          public void onError(final Exception error) {
            System.err.println("onError called!");
            error.printStackTrace();
          }

          @Override
          public void onCompleted() {
            System.out.println("onCompleted " + x);
          }

        });

    o = stage.wireIn();

    final WorkerThread[] workers = new WorkerThread[THREAD_COUNT];

    for (int i = 0; i < THREAD_COUNT; i++) {
      workers[i] = new WorkerThread();
    }
    final long start = System.currentTimeMillis();
    for (int i = 0; i < THREAD_COUNT; i++) {
      workers[i].start();
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
      workers[i].join();
    }
    o.onCompleted();

    final long inStop = System.currentTimeMillis();
    done = true;

    stage.close();
    final long outStop = System.currentTimeMillis();

    final long eventCount = THREAD_COUNT * EVENTS_PER_THREAD;
    final double inelapsed = ((double) (inStop - start)) / 1000.0;
    final double inoutelapsed = ((double) (outStop - start)) / 1000.0;
    System.out.println("Emitted " + eventCount + " events in " + inelapsed
        + " seconds (" + ((double) eventCount) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("Emitted/output " + eventCount + " events in " + inoutelapsed
        + " seconds (" + ((double) eventCount) / (inoutelapsed * 1000.0 * 1000.0) + " million events/sec)");

  }

  private class WorkerThread extends Thread {
    @Override
    public void run() {
      final Random rand = new Random();
      for (int i = 0; i < EVENTS_PER_THREAD; i++) {
        final int r = rand.nextInt(BUCKET_COUNT);
        o.onNext(new CombinerStage.Pair<Integer, Integer>(r, 1));
      }

    }
  }
}
