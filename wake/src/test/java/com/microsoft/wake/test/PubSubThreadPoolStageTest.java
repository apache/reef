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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.PubSubEventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.impl.TimerStage;
import com.microsoft.wake.test.util.Monitor;
import com.microsoft.wake.test.util.TimeoutHandler;


public class PubSubThreadPoolStageTest {
  
  @Rule public TestName name = new TestName();

  final String logPrefix = "TEST ";
  
  @Test
  public void testPubSubThreadPoolStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Monitor monitor = new Monitor();
    TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 5000, 5000);

    Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>()); 
    Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    int expected = 10;

    PubSubEventHandler<TestEvent> handler = new PubSubEventHandler<TestEvent>();
    handler.subscribe(TestEvent.class, new TestEventHandler("Handler1", monitor, procSet, expected));
    handler.subscribe(TestEvent.class, new TestEventHandler("Handler2", monitor, procSet, expected)); 
    
    EStage<TestEvent> stage = new ThreadPoolStage<TestEvent>(handler, 10);
    
    for (int i=0; i<expected; ++i) {
      TestEvent a = new TestEvent("aaa");
      orgSet.add(a);
      
      stage.onNext(a);
      
      if (i == 5) {
        handler.subscribe(TestEvent.class, new TestEventHandler("Handler3", monitor, procSet, expected)); 
      }
    }
    
    monitor.mwait();

    stage.close();
    timer.close();
    
    Assert.assertEquals(orgSet, procSet);
  }
  
  class TestEvent {
    private final String msg;
    public TestEvent(String msg) {
      this.msg = msg;
    }
    
    public String getMsg() {
      return msg;
    }
  }
  
  class TestEventHandler implements EventHandler<TestEvent> {

    private final String name;
    private final Monitor monitor;
    private final Set<TestEvent> set;
    private final int expected;

    TestEventHandler(String name, Monitor monitor, Set<TestEvent> set, int expected) {
      this.name = name;
      this.monitor = monitor;
      this.set = set;
      this.expected = expected;
    }
    
    @Override
    public void onNext(TestEvent e) {
      set.add(e);
      System.out.println(name + " " + e + " " + e.getMsg()); 
      if (set.size() == expected)
        monitor.mnotify();
    }
  }
  
}
