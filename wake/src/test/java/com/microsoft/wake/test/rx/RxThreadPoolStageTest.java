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
package com.microsoft.wake.test.rx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.RxStage;
import com.microsoft.wake.rx.impl.RxThreadPoolStage;


public class RxThreadPoolStageTest {
  
  @Rule public TestName name = new TestName();

  @Test
  public void testOne() throws Exception {
    System.out.println(name.getMethodName());
    
    TestObserver to = new TestObserver("o1");
    RxStage<TestEvent> stage = new RxThreadPoolStage<TestEvent>(to, 1);
    
    int i = 0;
    int sum = 0;
    try {
      for (i=0; i<20; ++i) {
        stage.onNext(new TestEvent(i));
        sum += i;
      }
      stage.onCompleted();
    } catch (Exception e) {
      stage.onError(e);
    }
        
    stage.close();
    
    Assert.assertEquals(1, to.getCompletes());
    Assert.assertEquals(0, to.getErrors());
    Assert.assertEquals(sum, to.getEvents());
  }

  @Test
  public void testMultipleThreads() throws Exception {
    System.out.println(name.getMethodName());
    
    TestObserver to = new TestObserver("o1");
    RxStage<TestEvent> stage = new RxThreadPoolStage<TestEvent>(to, 11);
    
    int i = 0;
    int sum = 0;
    try {
      for (i=0; i<20; ++i) {
        stage.onNext(new TestEvent(i));
        sum += i;
      }
      stage.onCompleted();
    } catch (Exception e) {
      stage.onError(e);
    }
        
    stage.close();
    
    Assert.assertEquals(1, to.getCompletes());
    Assert.assertEquals(0, to.getErrors());
    Assert.assertEquals(sum, to.getEvents());
  }

  @Test
  public void testMultipleCallers() throws Exception {
    System.out.println(name.getMethodName());
    
    TestObserver to = new TestObserver("o1");
    final RxStage<TestEvent> stage = new RxThreadPoolStage<TestEvent>(to, 11);
    
    final int tn = 7;
    ExecutorService taskmaster = Executors.newFixedThreadPool(tn);
    List<Future<Integer>> handles = new ArrayList<>();
    for (int t=0; t<tn; t++) {
      handles.add(taskmaster.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          int lsum = 0;
          for (int i=0; i<20; ++i) {
            stage.onNext(new TestEvent(i));
            lsum += i;
          }
          return lsum;
        }
      }));
    }
    int sum = 0;
    for (Future<Integer> h : handles) {
      try {
        sum+= h.get();
      } catch (InterruptedException | ExecutionException e1) {
        e1.printStackTrace();
        Assert.fail();
      }
    }

    try {
      stage.onCompleted();
    } catch (Exception e) {
      stage.onError(e);
    }
        
    stage.close();
   
    Assert.assertEquals(1, to.getCompletes());
    Assert.assertEquals(0, to.getErrors());
    Assert.assertEquals(sum, to.getEvents());
  }
    
  class TestEvent {
    private int n;
    TestEvent(int n) {
      this.n = n;
    }
    
    int get() {
      return n;
    }
  }
  
  class TestObserver implements Observer<TestEvent> {
    
    private final AtomicInteger events = new AtomicInteger(0);
    public int getEvents() {
      return events.get();
    }
    private final AtomicInteger errors = new AtomicInteger(0);
    public int getErrors() {
      return errors.get();
    }
    private final AtomicInteger completes = new AtomicInteger(0);
    public int getCompletes() {
      return completes.get();
    }
    
    private final String name;
    TestObserver(String name) {
      this.name = name;
    }
    
    @Override
    public void onNext(TestEvent value) {
      System.out.println(name + " Value: " + value + " " + value.get());
      events.addAndGet(value.get());
    }

    @Override
    public void onError(Exception error) {
      System.out.println(name + " Error: " + error);
      errors.incrementAndGet();
    }

    @Override
    public void onCompleted() {
      System.out.println(name + " Completed");
      completes.incrementAndGet();
    }
  };

}
