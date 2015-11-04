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

import org.junit.Assert;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for ConcurrentSkipListMap.
 */
public class SkipListTest {

  public static void main(final String[] arg) {
    final SkipListTest t = new SkipListTest();

    t.testPoll();
    t.testHigher();
    t.testHigherRemove();
    t.testHigherRemoveM();
    t.testHigherRemoveSeekM();
    t.testHigherRemoveSeekBoundedM();
    t.testHigherRemoveViewBoundedM();
    t.testSeparateMaps();
    t.testPollM();
  }

  //@Test
  public void testPoll() {
    System.out.println("poll");
    final int unique = 2000000;
    final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
    final long instart = System.currentTimeMillis();
    for (int i = 0; i < unique; i++) {
      x.put(i, i);
    }
    final long inend = System.currentTimeMillis();

    final long outstart = System.currentTimeMillis();
    while (x.pollFirstEntry() != null) {
      //
    }
    final long outend = System.currentTimeMillis();


    final double inelapsed = ((double) (inend - instart)) / 1000.0;
    final double outelapsed = ((double) (outend - outstart)) / 1000.0;
    System.out.println("insert " + unique + " events in " + inelapsed
        + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("output " + unique + " events in " + outelapsed
        + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
  }

  //@Test
  public void testHigher() {
    System.out.println("higher");
    final int unique = 2000000;
    final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
    final long instart = System.currentTimeMillis();
    for (int i = 0; i < unique; i++) {
      x.put(i, i);
    }
    final long inend = System.currentTimeMillis();

    System.gc();

    final long outstart = System.currentTimeMillis();
    Integer k = x.pollFirstEntry().getKey();
    while ((k = x.higherKey(k)) != null) {
      //
    }
    final long outend = System.currentTimeMillis();


    final double inelapsed = ((double) (inend - instart)) / 1000.0;
    final double outelapsed = ((double) (outend - outstart)) / 1000.0;
    System.out.println("insert " + unique + " events in " + inelapsed
        + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("output " + unique + " events in " + outelapsed
        + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
  }

  public boolean nThreads(final int n, final Runnable r, final long timeout, final TimeUnit t) {
    final ExecutorService e = Executors.newCachedThreadPool();
    for (int i = 0; i < n; i++) {
      e.submit(r);
    }
    e.shutdown();
    try {
      return e.awaitTermination(timeout, t);
    } catch (final InterruptedException e1) {
      e1.printStackTrace();
      return false;
    }
  }

  //@Test
  public void testHigherRemove() {
    System.out.println("higher/remove");
    final int unique = 2000000;
    final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
    final long instart = System.currentTimeMillis();
    for (int i = 0; i < unique; i++) {
      x.put(i, i);
    }
    final long inend = System.currentTimeMillis();

    System.gc();

    final long outstart = System.currentTimeMillis();
    Integer k = x.pollFirstEntry().getKey();
    x.remove(k);
    while ((k = x.higherKey(k)) != null) {
      x.remove(k);
    }
    final long outend = System.currentTimeMillis();


    final double inelapsed = ((double) (inend - instart)) / 1000.0;
    final double outelapsed = ((double) (outend - outstart)) / 1000.0;
    System.out.println("insert " + unique + " events in " + inelapsed
        + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("output " + unique + " events in " + outelapsed
        + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
  }

  //@Test
  public void testHigherRemoveM() {
    final int numOutW = 4;
    System.out.println("higher/remove " + numOutW);
    final int unique = 2000000;
    final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
    final long instart = System.currentTimeMillis();
    for (int i = 0; i < unique; i++) {
      x.put(i, i);
    }
    final long inend = System.currentTimeMillis();

    System.gc();

    final long outstart = System.currentTimeMillis();
    Assert.assertTrue(nThreads(numOutW, new Runnable() {

      @Override
      public void run() {
        Integer k = x.pollFirstEntry().getKey();
        x.remove(k);
        while ((k = x.higherKey(k)) != null) {
          x.remove(k);
        }
      }
    }, 30, TimeUnit.SECONDS));
    final long outend = System.currentTimeMillis();


    final double inelapsed = ((double) (inend - instart)) / 1000.0;
    final double outelapsed = ((double) (outend - outstart)) / 1000.0;
    System.out.println("insert " + unique + " events in " + inelapsed
        + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("output " + unique + " events in " + outelapsed
        + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
  }

  //@Test
  public void testHigherRemoveSeekM() {
    for (int numOutW = 1; numOutW <= 24; numOutW += 1) {
      System.out.println("higher/remove seek " + numOutW);
      final int unique = 2000000;
      final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
      final long instart = System.currentTimeMillis();
      for (int i = 0; i < unique; i++) {
        x.put(i, i);
      }
      final long inend = System.currentTimeMillis();

      System.gc();

      final long outstart = System.currentTimeMillis();
      final AtomicInteger uid = new AtomicInteger(0);
      final int blockSize = unique / numOutW;
      Assert.assertTrue(nThreads(numOutW, new Runnable() {

        @Override
        public void run() {
          final int id = uid.getAndIncrement();
          final Integer startK = x.ceilingKey(blockSize * id);
          Integer k = startK;
          x.remove(k);
          while ((k = x.higherKey(k)) != null) {
            x.remove(k);
          }
        }
      }, 30, TimeUnit.SECONDS));
      final long outend = System.currentTimeMillis();


      final double inelapsed = ((double) (inend - instart)) / 1000.0;
      final double outelapsed = ((double) (outend - outstart)) / 1000.0;
      System.out.println("insert " + unique + " events in " + inelapsed
          + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("output " + unique + " events in " + outelapsed
          + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
    }
  }

  //@Test
  public void testHigherRemoveSeekBoundedM() {
    for (int numOutW = 1; numOutW <= 24; numOutW += 1) {
      System.out.println("higher/remove seek " + numOutW);
      final int unique = 2000000;
      final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
      final long instart = System.currentTimeMillis();
      for (int i = 0; i < unique; i++) {
        x.put(i, i);
      }
      final long inend = System.currentTimeMillis();

      System.gc();

      final long outstart = System.currentTimeMillis();
      final AtomicInteger uid = new AtomicInteger(0);
      final int blockSize = unique / numOutW;
      Assert.assertTrue(nThreads(numOutW, new Runnable() {

        @Override
        public void run() {
          final int id = uid.getAndIncrement();
          final Integer startK = x.ceilingKey(blockSize * id);
          final Integer endK = x.ceilingKey(blockSize * (id + 1));
          Integer k = startK;
          do {
            x.remove(k);
            k = x.higherKey(k);
          } while (k != null && k < endK);
        }
      }, 30, TimeUnit.SECONDS));
      final long outend = System.currentTimeMillis();


      final double inelapsed = ((double) (inend - instart)) / 1000.0;
      final double outelapsed = ((double) (outend - outstart)) / 1000.0;
      System.out.println("insert " + unique + " events in " + inelapsed
          + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("output " + unique + " events in " + outelapsed
          + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
    }
  }

  //@Test
  public void testHigherRemoveViewBoundedM() {
    for (int numOutW = 1; numOutW <= 24; numOutW += 1) {
      System.out.println("higher/remove view " + numOutW);
      final int unique = 2000000;
      final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
      final long instart = System.currentTimeMillis();
      for (int i = 0; i < unique; i++) {
        x.put(i, i);
      }
      final long inend = System.currentTimeMillis();

      System.gc();

      final long outstart = System.currentTimeMillis();
      final AtomicInteger uid = new AtomicInteger(0);
      final int blockSize = unique / numOutW;
      Assert.assertTrue(nThreads(numOutW, new Runnable() {

        @Override
        public void run() {
          final int id = uid.getAndIncrement();
          final ConcurrentNavigableMap<Integer, Integer> myView = x.tailMap(blockSize * id);
          final Integer endK = x.ceilingKey(blockSize * (id + 1));
          Integer k = myView.pollFirstEntry().getKey();
          do {
            k = x.higherKey(k);
            if (k == null || k >= endK) {
              break;
            }
            x.remove(k);
          } while (true);
        }
      }, 30, TimeUnit.SECONDS));
      final long outend = System.currentTimeMillis();


      final double inelapsed = ((double) (inend - instart)) / 1000.0;
      final double outelapsed = ((double) (outend - outstart)) / 1000.0;
      System.out.println("insert " + unique + " events in " + inelapsed
          + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("output " + unique + " events in " + outelapsed
          + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
    }
  }

  //@Test
  @SuppressWarnings("checkstyle:avoidnestedblocks")
  public void testSeparateMaps() {
    for (int numOutW = 1; numOutW <= 24; numOutW += 1) {
      System.out.println("separate maps " + numOutW);
      final int unique = 2000000 / numOutW;
      final ConcurrentSkipListMap<Integer, Integer>[] x = new ConcurrentSkipListMap[numOutW];
      final long instart = System.currentTimeMillis();
      {
        final AtomicInteger uid = new AtomicInteger(0);
        nThreads(numOutW, new Runnable() {

          @Override
          public void run() {
            final ConcurrentSkipListMap<Integer, Integer> mm = new ConcurrentSkipListMap<>();
            final int u = uid.getAndIncrement();
            x[u] = mm;
            for (int i = 0; i < unique; i++) {
              mm.put(i, i);
            }
          }
        },
            10, TimeUnit.SECONDS);
      }
      final long inend = System.currentTimeMillis();

      System.gc();

      final long outstart = System.currentTimeMillis();
      final AtomicInteger uid = new AtomicInteger(0);
      Assert.assertTrue(nThreads(numOutW, new Runnable() {

        @Override
        public void run() {
          final int id = uid.getAndIncrement();
          final ConcurrentSkipListMap<Integer, Integer> mm = x[id];
          final Integer startK = mm.pollFirstEntry().getKey();
          Integer k = startK;
          mm.remove(k);
          while ((k = mm.higherKey(k)) != null) {
            mm.remove(k);
          }
        }
      }, 30, TimeUnit.SECONDS));
      final long outend = System.currentTimeMillis();

      final int total = unique * numOutW;
      final double inelapsed = ((double) (inend - instart)) / 1000.0;
      final double outelapsed = ((double) (outend - outstart)) / 1000.0;
      System.out.println("insert " + total + " events in " + inelapsed
          + " seconds (" + ((double) total) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("output " + total + " events in " + outelapsed
          + " seconds (" + ((double) total) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
    }
  }

  //@Test
  public void testPollM() {
    final int numOutW = 4;
    System.out.println("poll " + numOutW);
    final int unique = 2000000;
    final ConcurrentSkipListMap<Integer, Integer> x = new ConcurrentSkipListMap<>();
    final long instart = System.currentTimeMillis();
    for (int i = 0; i < unique; i++) {
      x.put(i, i);
    }
    final long inend = System.currentTimeMillis();

    System.gc();

    final long outstart = System.currentTimeMillis();
    Assert.assertTrue(nThreads(numOutW, new Runnable() {

      @Override
      public void run() {
        Integer k = x.pollFirstEntry().getKey();
        x.remove(k);
        while ((k = x.higherKey(k)) != null) {
          x.remove(k);
        }
      }
    }, 30, TimeUnit.SECONDS));
    final long outend = System.currentTimeMillis();


    final double inelapsed = ((double) (inend - instart)) / 1000.0;
    final double outelapsed = ((double) (outend - outstart)) / 1000.0;
    System.out.println("insert " + unique + " events in " + inelapsed
        + " seconds (" + ((double) unique) / (inelapsed * 1000.0 * 1000.0) + " million events/sec)");
    System.out.println("output " + unique + " events in " + outelapsed
        + " seconds (" + ((double) unique) / (outelapsed * 1000.0 * 1000.0) + " million events/sec)");
  }
}
