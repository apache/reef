/**
 * Copyright (C) 2012 Microsoft Corporation
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

import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.MergingEventHandler;
import com.microsoft.wake.impl.MergingEventHandler.Pair;

public class MergingEventHandlerTest {

  @Test
  public void testSingleInvocationSingleThread() {
    int testLeft = 13;
    int testRight = 23;
    int expected = testLeft + 31*testRight;
    
    final AtomicInteger i = new AtomicInteger(0);
    MergingEventHandler<Integer, Integer> dut = new MergingEventHandler<>(new EventHandler<Pair<Integer,Integer>>() {
      @Override
      public void onNext(Pair<Integer, Integer> value) {
        i.addAndGet(value.first+31*value.second);
      }
    });
    
    MergingEventHandler<Integer, Integer>.Left left = dut.new Left(); 
    MergingEventHandler<Integer, Integer>.Right right = dut.new Right(); 
    
    left.onNext(testLeft);
    right.onNext(testRight);
    assertEquals(expected, i.get());

  }  
 
  @Test
  public void testSingleInvocationSingleThreadReversed() {
    int testLeft = 11;
    int testRight = 47;
    int expected = testLeft + 17*testRight;
    
    final AtomicInteger i = new AtomicInteger(0);
    MergingEventHandler<Integer, Integer> dut = new MergingEventHandler<>(new EventHandler<Pair<Integer,Integer>>() {
      @Override
      public void onNext(Pair<Integer, Integer> value) {
        i.addAndGet(value.first+17*value.second);
      }
    });
    
    MergingEventHandler<Integer, Integer>.Left left = dut.new Left(); 
    MergingEventHandler<Integer, Integer>.Right right = dut.new Right(); 

    right.onNext(testRight);
    left.onNext(testLeft);
    assertEquals(expected, i.get());
  }

  @Test
  public void testMultipleInvocationSingleThread() {
    int testLeft1 = 13;
    int testRight1 = 23;
    int testLeft2 = 14;
    int testRight2 = 1001;
    int expected1 = testLeft1 + 31*testRight1;
    int expected2 = testLeft2 + 31*testRight2;
    
    final AtomicInteger i = new AtomicInteger(0);
    MergingEventHandler<Integer, Integer> dut = new MergingEventHandler<>(new EventHandler<Pair<Integer,Integer>>() {
      @Override
      public void onNext(Pair<Integer, Integer> value) {
        i.addAndGet(value.first+31*value.second);
      }
    });
    
    MergingEventHandler<Integer, Integer>.Left left = dut.new Left(); 
    MergingEventHandler<Integer, Integer>.Right right = dut.new Right(); 

    left.onNext(testLeft1);
    right.onNext(testRight1);
    
    left.onNext(testLeft2);
    right.onNext(testRight2);
    assertEquals(expected1+expected2, i.get());
  }  
  
  @Test
  public void testMultipleInvocationMultipleThread() {
    final int testLeft1 = 13;
    final int testRight1 = 23;
    final int testLeft2 = 14;
    final int testRight2 = 1001;
    final int expected1 = testLeft1 + 31*testRight1;
    final int expected2 = testLeft2 + 31*testRight2;
    
    final AtomicInteger i = new AtomicInteger(0);
    final MergingEventHandler<Integer, Integer> dut = new MergingEventHandler<>(new EventHandler<Pair<Integer,Integer>>() {
      @Override
      public void onNext(Pair<Integer, Integer> value) {
        i.addAndGet(value.first+31*value.second);
      }
    });
    
    final MergingEventHandler<Integer, Integer>.Left left = dut.new Left(); 
    final MergingEventHandler<Integer, Integer>.Right right = dut.new Right(); 

    // relies on Executor using both threads
    ExecutorService e = Executors.newFixedThreadPool(2);
    e.submit(new Runnable() {
      @Override
      public void run() {
        left.onNext(testLeft1);
        right.onNext(testRight2);
      }
    });

    e.submit(new Runnable() {
      @Override
      public void run() {
        right.onNext(testRight1);
        left.onNext(testLeft2);
      }
    });
    
    e.shutdown();
    try {
      e.awaitTermination(20, TimeUnit.SECONDS);
    } catch (InterruptedException e1) {
      fail("Timeout waiting for events to fire, perhaps due to deadlock");
    }
    
    assertEquals(expected1+expected2, i.get());
  }  

  @Test
  public void testManyInvocations() {
    final int expectedEvents = 200;
    final int numLeftTasks = 2;
    final int numRightTasks = 4;
    
    final int eventsPerLeft = expectedEvents/numLeftTasks;
    assertEquals("Test parameters must divide", expectedEvents, numLeftTasks*eventsPerLeft);
    final int eventsPerRight = expectedEvents/numRightTasks;
    assertEquals("Test parameters must divide", expectedEvents, numRightTasks*eventsPerRight);
    
    final AtomicInteger i = new AtomicInteger(0);
    final MergingEventHandler<Integer, Integer> dut = new MergingEventHandler<>(new EventHandler<Pair<Integer,Integer>>() {
      @Override
      public void onNext(Pair<Integer, Integer> value) {
        i.incrementAndGet();
      }
    });
    
    final MergingEventHandler<Integer, Integer>.Left left = dut.new Left(); 
    final MergingEventHandler<Integer, Integer>.Right right = dut.new Right(); 

    // relies on Executor making all tasks concurrent
    ExecutorService e = Executors.newCachedThreadPool();
    for (int l=0; l<numLeftTasks; l++) {
      e.submit(new Runnable() {
        @Override
        public void run() {
          for (int kk=0; kk<eventsPerLeft; kk++) {
            left.onNext(kk);
          }
        }
      });
    }
    for (int r=0; r<numRightTasks; r++) {
      e.submit(new Runnable() {
        @Override
        public void run() {
          for (int kk=0; kk<eventsPerRight; kk++) {
            right.onNext(kk);
          }
        }
      });
    }

    e.shutdown();
    try {
      e.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e1) {
      fail("Timeout waiting for events to fire, perhaps due to deadlock");
    }
    
    assertEquals(expectedEvents, i.get());
  }  
  
  @Test
  public void testDifferentTypes() {
    final AtomicInteger i = new AtomicInteger(0);
    MergingEventHandler<Boolean, Double> dut = new MergingEventHandler<>(new EventHandler<Pair<Boolean,Double>>() {
      @Override
      public void onNext(Pair<Boolean,Double> value) {
        i.incrementAndGet();
      }
    });
    MergingEventHandler<Boolean, Double>.Left left = dut.new Left(); 
    MergingEventHandler<Boolean, Double>.Right right = dut.new Right(); 

    left.onNext(true);
    right.onNext(104.0);
    
    assertEquals(1, i.get());
  }

}
