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
package org.apache.reef.wake.test;

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.*;


public class ThreadPoolStageTest {

  final String logPrefix = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testSingleThreadStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());

    EventHandler<TestEventA> eventHandler = new TestEventHandlerA(procSet);
    EStage<TestEventA> stage = new SingleThreadStage<TestEventA>(eventHandler, 20);

    for (int i = 0; i < 10; ++i) {
      TestEventA a = new TestEventA();
      orgSet.add(a);

      stage.onNext(a);
    }

    while (procSet.size() < 10) {
    }

    stage.close();

    Assert.assertEquals(orgSet, procSet);
  }

  @Test(expected = IllegalStateException.class)
  public void testSingleThreadStageQueueFull() {
    System.out.println(logPrefix + name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    final EventHandler<TestEventA> eventHandler = new TestEventHandlerA(procSet);
    final EStage<TestEventA> stage = new SingleThreadStage<TestEventA>(eventHandler, 1);

    for (int i = 0; i < 10000; ++i) {
      TestEventA a = new TestEventA();
      stage.onNext(a);
    }

    Assert.fail("No exception thrown");
  }

  @Test
  public void testThreadPoolStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());

    EventHandler<TestEventA> eventHandler = new TestEventHandlerA(procSet);

    EStage<TestEventA> stage = new ThreadPoolStage<TestEventA>(eventHandler, 10);

    for (int i = 0; i < 10; ++i) {
      TestEventA a = new TestEventA();
      orgSet.add(a);

      stage.onNext(a);
    }

    while (procSet.size() < 10) {
    }

    stage.close();

    Assert.assertEquals(orgSet, procSet);
  }

  @Test
  public void testMultiThreadPoolStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());

    Map<Class<? extends TestEvent>, EventHandler<? extends TestEvent>> map
        = new HashMap<Class<? extends TestEvent>, EventHandler<? extends TestEvent>>();
    map.put(TestEventA.class, new TestEventHandlerA(procSet));
    map.put(TestEventB.class, new TestEventHandlerB(procSet));
    EventHandler<TestEvent> eventHandler = new MultiEventHandler<TestEvent>(map);

    EStage<TestEvent> stage = new ThreadPoolStage<TestEvent>(eventHandler, 10);

    for (int i = 0; i < 10; ++i) {
      TestEventA a = new TestEventA();
      TestEventB b = new TestEventB();
      orgSet.add(a);
      orgSet.add(b);

      stage.onNext(a);
      stage.onNext(b);
    }

    while (procSet.size() < 20) {
    }

    stage.close();

    Assert.assertEquals(orgSet, procSet);
  }

  @Test
  public void testMeter() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    EventHandler<TestEvent> eventHandler = new TestEventHandler();
    ThreadPoolStage<TestEvent> stage = new ThreadPoolStage<TestEvent>(eventHandler, 10);

    TestEvent e = new TestEvent();
    for (int i = 0; i < 1000000; ++i) {
      stage.onNext(e);
    }
    stage.close();
    System.out.println("Note this is not a real test of throughput");
    System.out.println("mean input throughput: " + stage.getInMeter().getMeanThp() + " events/sec");
    System.out.println("mean output throughput: " + stage.getOutMeter().getMeanThp() + " events/sec");
  }

  @Test
  public void testMeterTwoStages() throws Exception {
    System.out.println(logPrefix + name.getMethodName());
    EventHandler<TestEvent> eventHandler = new TestEventHandler();
    ThreadPoolStage<TestEvent> stage2 = new ThreadPoolStage<TestEvent>(eventHandler, 5);
    ThreadPoolStage<TestEvent> stage1 = new ThreadPoolStage<TestEvent>(stage2, 5);

    TestEvent e = new TestEvent();
    for (int i = 0; i < 1000000; ++i) {
      stage1.onNext(e);
    }
    stage1.close();
    stage2.close();
    System.out.println("Note this is not a real test of throughput");
    System.out.println("1: mean input throughput: " + stage1.getInMeter().getMeanThp() + " events/sec");
    System.out.println("1: mean output throughput: " + stage1.getOutMeter().getMeanThp() + " events/sec");
    System.out.println("2: mean input throughput: " + stage2.getInMeter().getMeanThp() + " events/sec");
    System.out.println("2: mean output throughput: " + stage2.getOutMeter().getMeanThp() + " events/sec");
  }

  class TestEvent {
  }

  class TestEventA extends TestEvent {
  }

  class TestEventB extends TestEvent {
  }

  class TestEventHandler implements EventHandler<TestEvent> {

    TestEventHandler() {
    }

    public void onNext(TestEvent e) {
      // no op
    }
  }

  class TestEventHandlerA implements EventHandler<TestEventA> {
    private final Set<TestEvent> set;

    TestEventHandlerA(Set<TestEvent> set) {
      this.set = set;
    }

    public void onNext(TestEventA e) {
      set.add(e);
      System.out.println("TestEventHandlerA " + e);
    }
  }

  class TestEventHandlerB implements EventHandler<TestEventB> {
    private final Set<TestEvent> set;

    TestEventHandlerB(Set<TestEvent> set) {
      this.set = set;
    }

    public void onNext(TestEventB e) {
      set.add(e);
      System.out.println("TestEventHandlerB " + e);
    }
  }

}
