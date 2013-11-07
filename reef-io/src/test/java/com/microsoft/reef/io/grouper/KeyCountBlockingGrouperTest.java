/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io.grouper;

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Grouper.Combiner;
import com.microsoft.reef.io.grouper.Grouper.Extractor;
import com.microsoft.reef.io.grouper.Grouper.Partitioner;
import com.microsoft.reef.io.grouper.impl.CombiningBlockingGrouper;
import com.microsoft.reef.io.grouper.impl.KeyCountGrouperUtils.CountedPair;
import com.microsoft.reef.io.grouper.impl.KeyCountGrouperUtils.CountedPairExtractor;
import com.microsoft.reef.io.grouper.impl.KeyCountGrouperUtils.DownCountedPairCombiner;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.Observer;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class KeyCountBlockingGrouperTest {


  public static class IgnorePartitioner implements Partitioner<Integer> {
    @Inject
    public IgnorePartitioner() {
    }

    @Override
    public int partition(Integer k) {
      return 0;
    }
  }


  public static class NullHandler<T> implements EventHandler<T> {
    @Inject
    public NullHandler() {
    }

    @Override
    public void onNext(T arg0) {
      // do nothing
    }
  }

  // Test a grouper that blocks on a key
  @Test
  public void testBlocking() throws BindException, InjectionException {
    final AtomicInteger events = new AtomicInteger(0);
    final AtomicInteger val = new AtomicInteger(0);

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, CombiningBlockingGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, DownCountedPairCombiner.class);
    cb.bindNamedParameter(DownCountedPairCombiner.ValueCombiner.class, IntSumCombiner.class);
    // ConfigurationModule
    cb.bindImplementation(Extractor.class, CountedPairExtractor.class);

    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, "1");

    Observer<Tuple<Integer, CountedPair<Integer, Integer>>> out = new Observer<Tuple<Integer, CountedPair<Integer, Integer>>>() {

      @Override
      public void onCompleted() {
        Assert.fail("Not expected");
      }

      @Override
      public void onError(Exception arg0) {
        Assert.fail("Not expected");
      }

      @Override
      public void onNext(Tuple<Integer, CountedPair<Integer, Integer>> arg0) {
        events.incrementAndGet();
        val.addAndGet(arg0.getValue().value);
      }
    };

    Injector inj = Tang.Factory.getTang().newInjector(cb.build());
    inj.bindVolatileParameter(StageConfiguration.StageObserver.class, out);

    Observer<CountedPair<Integer, Integer>> grouper_in = inj.getInstance(CombiningBlockingGrouper.class);
    // expect
    grouper_in.onNext(new CountedPair<>(1, 0, 3)); // expect 3 counts for key 1

    grouper_in.onNext(new CountedPair<>(1, 2, 1));
    assertEquals(0, events.get());
    grouper_in.onNext(new CountedPair<>(1, 2, 1));
    assertEquals(0, events.get());
    grouper_in.onNext(new CountedPair<>(1, 2, 1));
    assertEquals(1, events.get());
    assertEquals(6, val.get());
  }

  @Test
  public void testBlockingMultipleKeys() throws BindException, InjectionException {
    final int numKeys = 10;
    final AtomicInteger[] events = new AtomicInteger[numKeys];
    for (int i = 0; i < numKeys; i++) events[i] = new AtomicInteger(0);
    final AtomicInteger val = new AtomicInteger(0);

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, CombiningBlockingGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, DownCountedPairCombiner.class); // TODO: test
    cb.bindNamedParameter(DownCountedPairCombiner.ValueCombiner.class, IntSumCombiner.class);
    // ConfigurationModule
    cb.bindImplementation(Extractor.class, CountedPairExtractor.class);

    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, "1");


    Observer<Tuple<Integer, CountedPair<Integer, Integer>>> out = new Observer<Tuple<Integer, CountedPair<Integer, Integer>>>() {

      @Override
      public void onCompleted() {
        Assert.fail("Not expected");
      }

      @Override
      public void onError(Exception arg0) {
        Assert.fail("Not expected");
      }

      @Override
      public void onNext(Tuple<Integer, CountedPair<Integer, Integer>> arg0) {
        events[arg0.getValue().key].incrementAndGet();
        val.addAndGet(arg0.getValue().value);
      }
    };

    Injector inj = Tang.Factory.getTang().newInjector(cb.build());
    inj.bindVolatileParameter(StageConfiguration.StageObserver.class, out);

    Observer<CountedPair<Integer, Integer>> grouper_in = inj.getInstance(CombiningBlockingGrouper.class);

    for (int i = 0; i < numKeys; i++) {
      // expect key i to have i+1 counts
      grouper_in.onNext(new CountedPair<>(i, 0, i + 1));
    }

    // key k gets k invocations, interleaved round robin
    for (int i = 0; i < numKeys; i++) {
      for (int j = numKeys - 1; j >= i; j--) {
        grouper_in.onNext(new CountedPair<>(j, 1, 1));
      }
    }

    // all should have been triggered once
    for (int i = 0; i < numKeys; i++) {
      assertEquals(1, events[i].get());
    }
  }

  // Test a grouper that blocks on a key, using multiple input threads
  @Test
  public void testBlockingMultipleProducers() throws InterruptedException, BindException, InjectionException {
    final AtomicInteger events = new AtomicInteger(0);
    final AtomicInteger val = new AtomicInteger(0);

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, CombiningBlockingGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, DownCountedPairCombiner.class); // TODO: test
    cb.bindNamedParameter(DownCountedPairCombiner.ValueCombiner.class, IntSumCombiner.class);
    // ConfigurationModule
    cb.bindImplementation(Extractor.class, CountedPairExtractor.class);

    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, "1");


    Observer<Tuple<Integer, CountedPair<Integer, Integer>>> out = new Observer<Tuple<Integer, CountedPair<Integer, Integer>>>() {

      @Override
      public void onCompleted() {
        Assert.fail("Not expected");
      }

      @Override
      public void onError(Exception arg0) {
        Assert.fail("Not expected");
      }

      @Override
      public void onNext(Tuple<Integer, CountedPair<Integer, Integer>> arg0) {
        events.incrementAndGet();
        val.addAndGet(arg0.getValue().value);
      }
    };

    Injector inj = Tang.Factory.getTang().newInjector(cb.build());
    inj.bindVolatileParameter(StageConfiguration.StageObserver.class, out);

    final Observer<CountedPair<Integer, Integer>> grouper_in = inj.getInstance(CombiningBlockingGrouper.class);

    final int numTasks = 100;
    // expect key 1 to get count numTasks
    grouper_in.onNext(new CountedPair<>(1, 0, numTasks));

    ExecutorService e = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numTasks; i++) {
      e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            grouper_in.onNext(new CountedPair<>(1, 1, 1));
          } catch (Exception e) {
            Assert.fail(e.toString());
          }
        }
      });
    }
    e.shutdown();
    e.awaitTermination(10, TimeUnit.SECONDS);

    assertEquals(1, events.get());

    // expect key 1 to see numTasks count
    grouper_in.onNext(new CountedPair<>(1, 0, numTasks));
    e = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numTasks; i++) {
      e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            grouper_in.onNext(new CountedPair<>(1, 1, 1));
          } catch (Exception e) {
            Assert.fail(e.toString());
          }
        }
      });
    }
    e.shutdown();
    e.awaitTermination(10, TimeUnit.SECONDS);

    assertEquals(2, events.get());
  }

  public static class IntSumCombiner<K extends Comparable<K>> implements Grouper.Combiner<Tuple<K, Integer>, K, Integer> {
    @Inject
    public IntSumCombiner() {
    }

    @Override
    public Integer combine(K key, Integer sofar, Integer val) {
      if (sofar != null) {
        return sofar + val;
      } else {
        return val;
      }
    }

    @Override
    public Tuple<K, Integer> generate(K key, Integer val) {
      return new Tuple<>(key, val);
    }

    @Override
    public boolean keyFinished(K key, Integer val) {
      return false;
    }
  }
}