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
package org.apache.reef.tests.watcher;

import org.apache.reef.io.watcher.EventStream;
import org.apache.reef.io.watcher.EventType;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class TestEventStream implements EventStream {

  private Map<EventType, AtomicInteger> eventCounter;

  @Inject
  private TestEventStream(){
    this.eventCounter = new HashMap<>();
    for (final EventType type : EventType.values()) {
      eventCounter.put(type, new AtomicInteger());
    }
  }

  @Override
  public void onEvent(final EventType type, final String jsonEncodedEvent) {
    eventCounter.get(type).incrementAndGet();
  }

  private void checkEqualTo(final EventType type, final int expectedNum) {
    final int actualNum = eventCounter.get(type).get();
    if (actualNum != expectedNum) {
      throw new RuntimeException("The expected number of " + type + " is "
          + expectedNum + " but " + actualNum + " times occurred");
    }
  }

  private void checkGreaterThan(final EventType type, final int num) {
    final int actualNum = eventCounter.get(type).get();
    if (actualNum < num) {
      throw new RuntimeException("The number of event " + type + " should be greater than " + num
          + " but " + actualNum + " times occurred");
    }
  }

  /**
   * This validation is called in WatcherTestDriver#RuntimeStopHandler, so RuntimeStop should not be guaranteed
   * to be called before this.
   */
  public void validate() {
    checkEqualTo(EventType.RuntimeStart, 1);
    checkEqualTo(EventType.StartTime, 1);
    checkEqualTo(EventType.AllocatedEvaluator, 2);
    checkEqualTo(EventType.FailedEvaluator, 1);
    checkEqualTo(EventType.ActiveContext, 2);
    checkEqualTo(EventType.FailedContext, 1);
    checkEqualTo(EventType.FailedTask, 1);
    checkEqualTo(EventType.RunningTask, 2);
    checkEqualTo(EventType.SuspendedTask, 1);
    checkGreaterThan(EventType.TaskMessage, 0);
    checkEqualTo(EventType.CompletedTask, 1);
    checkEqualTo(EventType.ClosedContext, 1);
    checkEqualTo(EventType.CompletedEvaluator, 1);
    checkEqualTo(EventType.StopTime, 1);

  }
}
