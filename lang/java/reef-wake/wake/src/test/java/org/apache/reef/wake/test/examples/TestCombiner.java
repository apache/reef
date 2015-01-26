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
package org.apache.reef.wake.test.examples;

import org.apache.reef.wake.examples.accumulate.CombinerStage;
import org.apache.reef.wake.examples.accumulate.CombinerStage.Combiner;
import org.apache.reef.wake.rx.Observer;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCombiner {

  int bucketCount = 1000;
  int bucketSleepMillis = 100;
  int threadCount = 1;
  int eventsCount = 2 * 1000000;
  int eventsPerThread = eventsCount / threadCount;

  Observer<Entry<Integer, Integer>> o;

  volatile boolean done = false;

  @Test
  public void test() throws Exception {
    CombinerStage<Integer, Integer> stage = new CombinerStage<Integer, Integer>(
        new Combiner<Integer, Integer>() {

          @Override
          public Integer combine(Integer key, Integer old, Integer cur) {
            if (old != null) {
              return old.intValue() + cur.intValue();
            } else {
              return cur;
            }
          }

        }, new Observer<Entry<Integer, Integer>>() {
      private AtomicInteger x = new AtomicInteger(0);

      @Override
      public void onNext(Entry<Integer, Integer> value) {
        System.out.println(value.getKey() + "=" + value.getValue());
        x.incrementAndGet();
        try {
          if (!done)
            Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }

      @Override
      public void onError(Exception error) {
        System.err.println("onError called!");
        error.printStackTrace();
      }

      @Override
      public void onCompleted() {
        System.out.println("onCompleted " + x);
      }

    });

    o = stage.wireIn();

    WorkerThread[] workers = new WorkerThread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      workers[i] = new WorkerThread();
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      workers[i].start();
    }
    for (int i = 0; i < threadCount; i++) {
      workers[i].join();
    }
    o.onCompleted();

    long inStop = System.currentTimeMillis();
    done = true;

    stage.close();
    long outStop = System.currentTimeMillis();

    long eventCount = threadCount * eventsPerThread;
    double inelapsed = ((double) (inStop - start)) / 1000.0;
    double inoutelapsed = ((double) (outStop - start)) / 1000.0;
    System.out.println("Emitted " + eventCount + " events in " + inelapsed
        + " seconds (" + ((double) eventCount) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("Emitted/output " + eventCount + " events in " + inoutelapsed
        + " seconds (" + ((double) eventCount) / (inoutelapsed * 1000.0 * 1000.0) + " million events/sec)");

  }

  ;

  private class WorkerThread extends Thread {
    @Override
    public void run() {
      Random rand = new Random();
      for (int i = 0; i < eventsPerThread; i++) {
        int r = rand.nextInt(bucketCount);
        o.onNext(new CombinerStage.Pair<Integer, Integer>(r, 1));
      }

    }
  }
}
