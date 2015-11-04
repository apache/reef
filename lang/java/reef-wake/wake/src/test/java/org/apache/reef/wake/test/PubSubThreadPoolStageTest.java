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
package org.apache.reef.wake.test;

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.PubSubEventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.test.util.Monitor;
import org.apache.reef.wake.test.util.TimeoutHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Publish/subscribe event handler tests.
 */
public class PubSubThreadPoolStageTest {

  private static final String LOG_PREFIX = "TEST ";
  @Rule
  public TestName name = new TestName();

  @Test
  public void testPubSubThreadPoolStage() throws Exception {
    System.out.println(LOG_PREFIX + name.getMethodName());

    final Monitor monitor = new Monitor();
    final TimerStage timer = new TimerStage(new TimeoutHandler(monitor), 5000, 5000);

    final Set<TestEvent> procSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    final Set<TestEvent> orgSet = Collections.synchronizedSet(new HashSet<TestEvent>());
    final int expected = 10;

    final PubSubEventHandler<TestEvent> handler = new PubSubEventHandler<>();
    handler.subscribe(TestEvent.class, new TestEventHandler("Handler1", monitor, procSet, expected));
    handler.subscribe(TestEvent.class, new TestEventHandler("Handler2", monitor, procSet, expected));

    final EStage<TestEvent> stage = new ThreadPoolStage<>(handler, 10);

    for (int i = 0; i < expected; ++i) {
      final TestEvent a = new TestEvent("aaa");
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

    TestEvent(final String msg) {
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

    TestEventHandler(final String name, final Monitor monitor, final Set<TestEvent> set, final int expected) {
      this.name = name;
      this.monitor = monitor;
      this.set = set;
      this.expected = expected;
    }

    @Override
    public void onNext(final TestEvent e) {
      set.add(e);
      System.out.println(name + " " + e + " " + e.getMsg());
      if (set.size() == expected) {
        monitor.mnotify();
      }
    }
  }

}
