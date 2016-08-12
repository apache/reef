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
package org.apache.reef.wake.test.time.util;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * An EventHandler that records the events that it sees.
 * Can optionally count down the latch on each alarm it receives.
 * This is a helper class to be used in unit tests, e.g. RuntimeClockTest.
 */
public final class EventRecorder implements EventHandler<Alarm> {

  /**
   * A synchronized List of the events recorded by this EventRecorder.
   */
  private final List<Time> events = Collections.synchronizedList(new ArrayList<Time>());

  private final CountDownLatch eventCountLatch;

  public EventRecorder() {
    this(null);
  }

  /**
   * Create a new event recorder.
   * If latch is not null, count it down on each event received.
   * @param latch A count down latch. Can be null.
   */
  public EventRecorder(final CountDownLatch latch) {
    this.eventCountLatch = latch;
  }

  /**
   * Get the number of events captured so far.
   * @return A number of events captured.
   */
  public int getEventCount() {
    return this.events.size();
  }

  /**
   * Get the list of events captured so far, in the order they were captured.
   * @return A list of events. It can be empty, but never null.
   */
  public List<Time> getEvents() {
    return this.events;
  }

  /**
   * Add a new event to the list.
   * @param event An event to capture.
   */
  @Override
  public void onNext(final Alarm event) {
    this.events.add(event);
    if (this.eventCountLatch != null) {
      this.eventCountLatch.countDown();
    }
  }
}
