package com.microsoft.reef.io.grouper;
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

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Grouper.Combiner;
import com.microsoft.reef.io.grouper.Grouper.Extractor;
import com.microsoft.reef.io.grouper.Grouper.Partitioner;
import com.microsoft.reef.io.grouper.impl.AppendingSnowshovelGrouper;
import com.microsoft.reef.io.grouper.impl.CombiningBlockingGrouper;
import com.microsoft.reef.io.grouper.impl.CombiningSnowshovelGrouper;
import com.microsoft.reef.io.grouper.impl.SnowshovelGrouper;
import com.microsoft.reef.io.grouper.impl.SynchronousGrouper;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.RxStage;
import org.junit.Test;

import javax.inject.Inject;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestGrouper {
  private static Logger logger = Logger.getLogger(TestGrouper.class.getName());
 
  // shared configs
  final static int bucketCount = 1000;
  final static int eventsCount_combining = 40 * 1000000;
  final static int eventsCount_noncombining = 5 * 1000000;
  final static int workerCount = 24;
  final static int eventsPerWorker_combining = eventsCount_combining / workerCount;
  final static int eventsPerWorker_noncombining = eventsCount_noncombining / workerCount;
  final static int outputWorkerCount = 8;

  /**
   * Test for groupers with Tuple<Integer,Integer> combining
   */
  public static class TestIntegerSumGrouper {
    boolean outputStageDriverDone = false;
    Object outputStageDriversLock = new Object();
    final RxStage<Tuple<Integer,Integer>> grouper;

    /**
     * For now this constructor must be called before the constructor for the
     * Grouper under test
     */
    @Inject
    public TestIntegerSumGrouper(Grouper<Tuple<Integer,Integer>> grouper) {
      this.grouper = grouper;
    }


//    Observer<Tuple<Integer, Integer>> grouper_in;
//    Observer<Object> grouper_out;

    public void test(
        int numWorkers, int eventsPerWorker) throws Exception {
      logger.log(Level.INFO, "Test " + TestIntegerSumGrouper.class.getName() + " on " + grouper.getClass().getName());

      Thread[] workers = new Thread[numWorkers];
      for (int i = 0; i < numWorkers; i++)
        workers[i] = new WorkerThread(eventsPerWorker);

      long start, stopIn, stopOut;
      start = System.currentTimeMillis();
      {
        for (int i = 0; i < numWorkers; i++)
          workers[i].start();
        for (int i = 0; i < numWorkers; i++)
          workers[i].join();

        logger.log(Level.INFO, "Input side done");
        grouper.onCompleted();
        // here the input stage is finished
        stopIn = System.currentTimeMillis();

        // now just wait for output stage to finish
        logger.info("waiting in instance " + this);
        synchronized (outputStageDriversLock) {
          while (!outputStageDriverDone) {
            outputStageDriversLock.wait();
          }
        }
        grouper.close();

      }
      stopOut = System.currentTimeMillis();

      long actualEventCount = workerCount * eventsPerWorker;
      double elapsedBoth = ((double) (stopOut - start)) / 1000.0;
      double elapsedOut = ((double) (stopOut - stopIn)) / 1000.0;
      double elapsedIn = ((double) (stopIn - start)) / 1000.0;
      System.out.println("Input " + actualEventCount + " events in " + elapsedIn + " seconds ("
          + ((double) actualEventCount) / (elapsedIn * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("Output " + actualEventCount + " events in " + elapsedOut + " seconds ("
          + ((double) actualEventCount) / (elapsedOut * 1000.0 * 1000.0) + " million events/sec)");
      System.out.println("Input&Emitted " + actualEventCount + " events in " + elapsedBoth + " seconds ("
          + ((double) actualEventCount) / (elapsedBoth * 1000.0 * 1000.0) + " million events/sec)");
    }


    // variables for caching whether we have seen the timer stage trigger or not
    ThreadLocal<Boolean> first = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
        return false;
      }
    };
    AtomicBoolean firstShared = new AtomicBoolean(false);

    /*
     * shutdown the timer stage driving the Grouper.OutObserver and notify main
     * thread waiting on output finishing
     */
    public void closeDriverStage() {
      logger.log(Level.INFO, Thread.currentThread().getId()+ ": Timer stage for output is closing");
      try {
        synchronized (outputStageDriversLock) {
          outputStageDriverDone = true;
          outputStageDriversLock.notify();
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.toString());
      }
    }

    // sends a bunch of random keys to the combiner
    private class WorkerThread extends Thread {
      private final int eventsPerWorker;
      @Override
      public void run() {
        Random rand = new Random();
        for (int i = 0; i < eventsPerWorker; i++) {
          int r = rand.nextInt(bucketCount);
          // int r = cur;
          // cur = (cur == bucketCount-1) ? 0 : cur+1;
          grouper.onNext(new Tuple<Integer, Integer>(r, 1));
        }
      }

      public WorkerThread(int eventsPerWorker) {
        this.eventsPerWorker = eventsPerWorker;
      }

      // private int cur;
    }

    ;
  }

  /*
   * Test instantiations
   */

  @Test
  public void testShuffle() throws Exception {
    final int numWorkers = 4;

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, CombiningBlockingGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, IntCombiner.class); // TODO: test
    // ConfigurationModule
    cb.bindImplementation(Extractor.class, IntExtractor.class);
    
    // grouper has internal stage
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, Integer.toString(outputWorkerCount));

    // currently just here to share the instance between other injected
    // constructors
    cb.bindImplementation(TestIntegerSumGrouper.class, TestIntegerSumGrouper.class);
   
    // output checker
    cb.bindNamedParameter(OutputChecker.NumWorkers.class, Integer.toString(numWorkers));
    cb.bindNamedParameter(OutputChecker.CheckKeyCount.class, Boolean.toString(true));
    cb.bindNamedParameter(OutputChecker.NoCombiningExpected.class, Boolean.toString(false));
    cb.bindNamedParameter(OutputChecker.EventsPerWorker.class, Integer.toString(eventsPerWorker_combining));
    cb.bindNamedParameter(StageConfiguration.StageObserver.class, OutputChecker.class);
    
    Injector i = Tang.Factory.getTang().newInjector(cb.build());


    TestIntegerSumGrouper t = i.getInstance(TestIntegerSumGrouper.class);
    
    OutputChecker checker = i.getInstance(OutputChecker.class);
    checker.setTest(t);
    
    t.test(numWorkers, eventsPerWorker_combining);
  }

  @Test
  public void testSnowshovel() throws Exception {
    final int numWorkers = 4;

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, CombiningSnowshovelGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, IntCombiner.class); 
    cb.bindImplementation(Extractor.class, IntExtractor.class);

    // grouper has internal stage
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, Integer.toString(outputWorkerCount));

    // currently just here to share the instance between other injected constructors
    cb.bindImplementation(TestIntegerSumGrouper.class, TestIntegerSumGrouper.class);

    // output checker
    cb.bindNamedParameter(OutputChecker.NumWorkers.class, Integer.toString(numWorkers));
    cb.bindNamedParameter(OutputChecker.CheckKeyCount.class, Boolean.toString(false));
    cb.bindNamedParameter(OutputChecker.NoCombiningExpected.class, Boolean.toString(false));
    cb.bindNamedParameter(OutputChecker.EventsPerWorker.class, Integer.toString(eventsPerWorker_combining));
    cb.bindNamedParameter(StageConfiguration.StageObserver.class, OutputChecker.class);

    Injector i = Tang.Factory.getTang().newInjector(cb.build());


    TestIntegerSumGrouper t = i.getInstance(TestIntegerSumGrouper.class);

    OutputChecker checker = i.getInstance(OutputChecker.class);
    checker.setTest(t);
    
    t.test(numWorkers, eventsPerWorker_combining);
  }
  
  
  @Test
  public void testSnowshovelAppending() throws Exception {
    final int numWorkers = 4;

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, AppendingSnowshovelGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, ListCombiner.class); 
    cb.bindImplementation(Extractor.class, IntExtractor.class);

    // grouper has internal stage
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, Integer.toString(outputWorkerCount));

    // currently just here to share the instance between other injected constructors
    cb.bindImplementation(TestIntegerSumGrouper.class, TestIntegerSumGrouper.class);

    // output checker
    cb.bindNamedParameter(ListOutputChecker.NumWorkers.class, Integer.toString(numWorkers));
    cb.bindNamedParameter(ListOutputChecker.CheckKeyCount.class, Boolean.toString(false));
    cb.bindNamedParameter(ListOutputChecker.NoCombiningExpected.class, Boolean.toString(false));
    cb.bindNamedParameter(ListOutputChecker.EventsPerWorker.class, Integer.toString(eventsPerWorker_combining));
    cb.bindNamedParameter(StageConfiguration.StageObserver.class, ListOutputChecker.class);

    Injector i = Tang.Factory.getTang().newInjector(cb.build());


    TestIntegerSumGrouper t = i.getInstance(TestIntegerSumGrouper.class);

    ListOutputChecker checker = i.getInstance(ListOutputChecker.class);
    checker.setTest(t);
    
    t.test(numWorkers, eventsPerWorker_combining);
  }
  
  
  @Test
  public void testNonCombiningSnowshovel() throws Exception {
    final int numWorkers = 4;

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, SnowshovelGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, IntCombiner.class); 
    cb.bindImplementation(Extractor.class, IntExtractor.class);

    // grouper has internal stage
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, Integer.toString(outputWorkerCount));

    // currently just here to share the instance between other injected constructors
    cb.bindImplementation(TestIntegerSumGrouper.class, TestIntegerSumGrouper.class);

    // output checker
    cb.bindNamedParameter(OutputChecker.NumWorkers.class, Integer.toString(numWorkers));
    cb.bindNamedParameter(OutputChecker.CheckKeyCount.class, Boolean.toString(false));
    cb.bindNamedParameter(OutputChecker.NoCombiningExpected.class, Boolean.toString(true));
    cb.bindNamedParameter(OutputChecker.EventsPerWorker.class, Integer.toString(eventsPerWorker_noncombining));
    cb.bindNamedParameter(StageConfiguration.StageObserver.class, OutputChecker.class);

    Injector i = Tang.Factory.getTang().newInjector(cb.build());


    TestIntegerSumGrouper t = i.getInstance(TestIntegerSumGrouper.class);

    OutputChecker checker = i.getInstance(OutputChecker.class);
    checker.setTest(t);
    
    t.test(numWorkers, eventsPerWorker_noncombining);
  }

  @Test
  public void testNonCombiningSync() throws Exception {
    final int numWorkers = 4;

    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    cb.bindImplementation(Grouper.class, SynchronousGrouper.class);
    cb.bindImplementation(Partitioner.class, IgnorePartitioner.class);
    cb.bindImplementation(Combiner.class, IntCombiner.class); 
    cb.bindImplementation(Extractor.class, IntExtractor.class);

    // grouper has internal stage
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, Integer.toString(outputWorkerCount));

    // currently just here to share the instance between other injected constructors
    cb.bindImplementation(TestIntegerSumGrouper.class, TestIntegerSumGrouper.class);

    // output checker
    cb.bindNamedParameter(OutputChecker.NumWorkers.class, Integer.toString(numWorkers));
    cb.bindNamedParameter(OutputChecker.CheckKeyCount.class, Boolean.toString(false));
    cb.bindNamedParameter(OutputChecker.NoCombiningExpected.class, Boolean.toString(true));
    cb.bindNamedParameter(OutputChecker.EventsPerWorker.class, Integer.toString(eventsPerWorker_noncombining));
    cb.bindNamedParameter(StageConfiguration.StageObserver.class, OutputChecker.class);

    Injector i = Tang.Factory.getTang().newInjector(cb.build());


    TestIntegerSumGrouper t = i.getInstance(TestIntegerSumGrouper.class);

    OutputChecker checker = i.getInstance(OutputChecker.class);
    checker.setTest(t);
    
    t.test(numWorkers, eventsPerWorker_noncombining);
  }

  public static class IntCombiner implements Grouper.Combiner<Tuple<Integer,Integer>, Integer, Integer> {
    @Inject
    public IntCombiner() {
    }

    @Override
    public Integer combine(Integer key, Integer sofar, Integer val) {
      if (sofar != null) {
        return sofar + val;
      } else {
        return val;
      }
    }

    @Override
    public Tuple<Integer, Integer> generate(Integer key, Integer val) {
      return new Tuple<>(key, val);
    }

    @Override
    public boolean keyFinished(Integer key, Integer val) {
      return false;
    }
  }
  
  public static class ListCombiner implements Grouper.Combiner<Tuple<Integer,List<Integer>>, Integer, List<Integer>> {
    @Inject
    public ListCombiner() {
    }

    @Override
    public List<Integer> combine(Integer key, List<Integer> sofar, List<Integer> val) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Tuple<Integer, List<Integer>> generate(Integer key, List<Integer> val) {
      return new Tuple<>(key, val);
    }

    @Override
    public boolean keyFinished(Integer key, List<Integer> val) {
      return false;
    }
  }
  
  public static class IntExtractor implements Grouper.Extractor<Tuple<Integer, Integer>, Integer, Integer> {
    @Inject
    public IntExtractor() {
    }

    @Override
    public Integer key(Tuple<Integer, Integer> t) {
      return t.getKey();
    }

    @Override
    public Integer value(Tuple<Integer, Integer> t) {
      return t.getValue();
    }
  }

  public static class IgnorePartitioner implements Grouper.Partitioner<Integer> {
    @Inject
    public IgnorePartitioner() {
    }

    @Override
    public int partition(Integer k) {
      return 1;
    }

  }
  
  
  public static class OutputChecker implements Observer<Tuple<Integer,Tuple<Integer,Integer>>> {

    private AtomicInteger sum = new AtomicInteger(0);
    private AtomicInteger aggregatedEventCount = new AtomicInteger(0);
    private final int numWorkers;
    private TestIntegerSumGrouper test;
    private final boolean checkKeyCount;
    private final boolean noCombiningExpected;
    private final int eventsPerWorker;
    
    @NamedParameter
    public static class NumWorkers implements Name<Integer>{}
    @NamedParameter
    public static class CheckKeyCount implements Name<Boolean>{}
    @NamedParameter
    public static class NoCombiningExpected implements Name<Boolean>{}
    @NamedParameter
    public static class EventsPerWorker implements Name<Integer>{}
    
    @Inject
    public OutputChecker(@Parameter(NumWorkers.class) int numWorkers,
                         @Parameter(EventsPerWorker.class) int eventsPerWorker, 
                         @Parameter(CheckKeyCount.class) boolean checkKeyCount,
                         @Parameter(NoCombiningExpected.class) boolean noCombiningExpected) {
      this.numWorkers = numWorkers;
      this.checkKeyCount = checkKeyCount;
      this.noCombiningExpected = noCombiningExpected;
      this.eventsPerWorker = eventsPerWorker;
    }

    @Override
    public void onCompleted() {
      assertEquals(eventsPerWorker * numWorkers, sum.get());

      if (checkKeyCount) {
      // for blocking, all buckets should have been seen once if
      // astronomically low probability of missing one
        if (Math.pow(((double) bucketCount - 1) / bucketCount, eventsPerWorker*numWorkers) <= Double.MIN_VALUE) {
          assertEquals(aggregatedEventCount.get(), bucketCount);
        }
      }
      
      if (noCombiningExpected) {
        assertEquals(eventsPerWorker * numWorkers, aggregatedEventCount.get());
      }
      
      // clean up
      test.closeDriverStage();
    }

    @Override
    public void onError(Exception e) {
      fail("Error during grouper output: " + e);
    }

    @Override
    public void onNext(Tuple<Integer, Tuple<Integer, Integer>> t) {
      aggregatedEventCount.incrementAndGet();
      sum.addAndGet(t.getValue().getValue());
    }
    
    public void setTest(TestIntegerSumGrouper t) {
      test = t;
    }
  }

  public static class ListOutputChecker implements Observer<Tuple<Integer,Tuple<Integer,List<Integer>>>> {

    private AtomicInteger sum = new AtomicInteger(0);
    private AtomicInteger aggregatedEventCount = new AtomicInteger(0);
    private final int numWorkers;
    private TestIntegerSumGrouper test;
    private final boolean checkKeyCount;
    private final boolean noCombiningExpected;
    private final int eventsPerWorker;
    
    @NamedParameter
    public static class NumWorkers implements Name<Integer>{}
    @NamedParameter
    public static class CheckKeyCount implements Name<Boolean>{}
    @NamedParameter
    public static class NoCombiningExpected implements Name<Boolean>{}
    @NamedParameter
    public static class EventsPerWorker implements Name<Integer>{}
    
    @Inject
    public ListOutputChecker(@Parameter(NumWorkers.class) int numWorkers,
                         @Parameter(EventsPerWorker.class) int eventsPerWorker, 
                         @Parameter(CheckKeyCount.class) boolean checkKeyCount,
                         @Parameter(NoCombiningExpected.class) boolean noCombiningExpected) {
      this.numWorkers = numWorkers;
      this.checkKeyCount = checkKeyCount;
      this.noCombiningExpected = noCombiningExpected;
      this.eventsPerWorker = eventsPerWorker;
    }

    @Override
    public void onCompleted() {
      assertEquals(eventsPerWorker * numWorkers, sum.get());

      if (checkKeyCount) {
      // for blocking, all buckets should have been seen once if
      // astronomically low probability of missing one
        if (Math.pow(((double) bucketCount - 1) / bucketCount, eventsPerWorker*numWorkers) <= Double.MIN_VALUE) {
          assertEquals(aggregatedEventCount.get(), bucketCount);
        }
      }
      
      if (noCombiningExpected) {
        assertEquals(eventsPerWorker * numWorkers, aggregatedEventCount.get());
      }
      
      // clean up
      test.closeDriverStage();
    }

    @Override
    public void onError(Exception e) {
      fail("Error during grouper output: " + e);
    }

    @Override
    public void onNext(Tuple<Integer, Tuple<Integer, List<Integer>>> t) {
      aggregatedEventCount.incrementAndGet();
      for (int x : t.getValue().getValue()) {
        sum.addAndGet(x);
      }
    }
    
    public void setTest(TestIntegerSumGrouper t) {
      test = t;
    }
  }

}