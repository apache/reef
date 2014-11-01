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

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.PeriodicEvent;
import org.apache.reef.wake.impl.TimerStage;
import org.apache.reef.wake.test.util.Monitor;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.atomic.AtomicInteger;


public class TimerStageTest {

  final String logPrefix = "TEST ";
  final long shutdownTimeout = 1000;
  @Rule
  public TestName name = new TestName();

  @Test
  public void testTimerStage() throws Exception {
    System.out.println(logPrefix + name.getMethodName());

    Monitor monitor = new Monitor();
    int expected = 10;

    TestEventHandler handler = new TestEventHandler(monitor, expected);
    Stage stage = new TimerStage(handler, 100, shutdownTimeout);

    monitor.mwait();

    stage.close();

    Assert.assertEquals(expected, handler.getCount());
  }


  class TestEventHandler implements EventHandler<PeriodicEvent> {
    private final Monitor monitor;
    private final int expected;
    private AtomicInteger count = new AtomicInteger(0);

    TestEventHandler(Monitor monitor, int expected) {
      this.monitor = monitor;
      this.expected = expected;
    }

    public void onNext(PeriodicEvent e) {
      count.incrementAndGet();
      System.out.println(count.get() + " " + e + " scheduled event at " + System.currentTimeMillis());
      if (count.get() == expected)
        monitor.mnotify();
    }

    public int getCount() {
      return count.get();
    }
  }
}


