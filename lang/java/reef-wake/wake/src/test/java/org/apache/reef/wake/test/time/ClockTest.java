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
package org.apache.reef.wake.test.time;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.LogicalTimer;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Tests for Clock.
 */
public class ClockTest {

  private static RuntimeClock buildClock() throws Exception {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang()
            .newConfigurationBuilder();

    final Injector injector = Tang.Factory.getTang()
            .newInjector(builder.build());

    return injector.getInstance(RuntimeClock.class);
  }

  private static RuntimeClock buildLogicalClock() throws Exception {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang()
        .newConfigurationBuilder();

    builder.bind(Timer.class, LogicalTimer.class);

    final Injector injector = Tang.Factory.getTang()
        .newInjector(builder.build());
    return injector.getInstance(RuntimeClock.class);
  }

  @Test
  public void testClock() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final int minEvents = 40;
    final CountDownLatch eventCountLatch = new CountDownLatch(minEvents);

    final RuntimeClock clock = buildClock();
    new Thread(clock).start();
    final RandomAlarmProducer alarmProducer = new RandomAlarmProducer(clock, eventCountLatch);

    try (ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(alarmProducer, 10)) {
      stage.onNext(null);
      Assert.assertTrue(eventCountLatch.await(10, TimeUnit.SECONDS));
    } finally {
      clock.close();
    }
  }

  @Test
  public void testAlarmRegistrationRaceConditions() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final RuntimeClock clock = buildClock();
    new Thread(clock).start();

    final EventRecorder earlierAlarmRecorder = new EventRecorder();
    final EventRecorder laterAlarmRecorder = new EventRecorder();

    try {
      // Schedule an Alarm that's far in the future
      clock.scheduleAlarm(5000, laterAlarmRecorder);
      Thread.sleep(1000);

      // By now, RuntimeClockImpl should be in a timed wait() for 5000 ms.
      // Scheduler an Alarm that should fire before the existing Alarm:
      clock.scheduleAlarm(2000, earlierAlarmRecorder);
      Thread.sleep(1000);

      // The earlier Alarm shouldn't have fired yet (we've only slept 1/2 time):
      Assert.assertEquals(0, earlierAlarmRecorder.getEventCount());
      Thread.sleep(1500);

      // The earlier Alarm should have fired, since 3500 > 2000 ms have passed:
      Assert.assertEquals(1, earlierAlarmRecorder.getEventCount());
      // And the later Alarm shouldn't have fired yet:
      Assert.assertEquals(0, laterAlarmRecorder.getEventCount());
      Thread.sleep(2500);

      // The later Alarm should have fired, since 6000 > 5000 ms have passed:
      Assert.assertEquals(1, laterAlarmRecorder.getEventCount());
    } finally {
      clock.close();
    }
  }

  @Test
  public void testMultipleCloseCalls() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final int numThreads = 3;
    final CountDownLatch eventCountLatch = new CountDownLatch(numThreads);

    final RuntimeClock clock = buildClock();
    new Thread(clock).start();
    final ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm value) {
        clock.close();
        eventCountLatch.countDown();
      }
    }, numThreads);

    try {
      for (int i = 0; i < numThreads; ++i) {
        stage.onNext(null);
      }
      Assert.assertTrue(eventCountLatch.await(10, TimeUnit.SECONDS));
    } finally {
      stage.close();
      clock.close();
    }
  }

  @Test
  public void testSimultaneousAlarms() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final int expectedEvent = 2;
    final CountDownLatch eventCountLatch = new CountDownLatch(expectedEvent);

    final RuntimeClock clock = buildLogicalClock();
    new Thread(clock).start();

    final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);
    try {
      clock.scheduleAlarm(500, alarmRecorder);
      clock.scheduleAlarm(500, alarmRecorder);
      eventCountLatch.await(10, TimeUnit.SECONDS);
      Assert.assertEquals(expectedEvent, alarmRecorder.getEventCount());
    } finally {
      clock.close();
    }
  }

  @Test
  public void testAlarmOrder() throws Exception {
    LoggingUtils.setLoggingLevel(Level.FINE);

    final int numAlarms = 10;
    final CountDownLatch eventCountLatch = new CountDownLatch(numAlarms);

    final RuntimeClock clock = buildLogicalClock();
    new Thread(clock).start();

    final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);
    try {
      final long[] expected = new long[numAlarms];
      for (int i = 0; i < numAlarms; ++i) {
        clock.scheduleAlarm(i * 100, alarmRecorder);
        expected[i] = i * 100;
      }

      eventCountLatch.await(10, TimeUnit.SECONDS);

      final Long[] actualLong = new Long[numAlarms];
      alarmRecorder.getTimestamps().toArray(actualLong);
      final long[] actual = new long[numAlarms];
      for (int i = 0; i < numAlarms; ++i) {
        actual[i] = actualLong[i];
      }
      Assert.assertArrayEquals(expected, actual);
    } finally {
      clock.close();
    }
  }

  /**
   * An EventHandler that records the events that it sees.
   */
  private static class EventRecorder implements EventHandler<Alarm> {

    /**
     * A synchronized List of the events recorded by this EventRecorder.
     */
    private final List<Time> events = Collections.synchronizedList(new ArrayList<Time>());
    private final List<Long> timestamps = Collections.synchronizedList(new ArrayList<Long>());

    private final CountDownLatch eventCountLatch;

    EventRecorder() {
      this(null);
    }

    EventRecorder(final CountDownLatch latch) {
      eventCountLatch = latch;
    }

    public int getEventCount() {
      return events.size();
    }

    public List<Long> getTimestamps() {
      return timestamps;
    }

    @Override
    public void onNext(final Alarm event) {
      timestamps.add(event.getTimeStamp());
      events.add(event);
      if (eventCountLatch != null) {
        eventCountLatch.countDown();
      }
    }
  }

  private static class RandomAlarmProducer implements EventHandler<Alarm> {

    private final RuntimeClock clock;
    private final CountDownLatch eventCountLatch;
    private final Random rand;

    RandomAlarmProducer(final RuntimeClock clock, final CountDownLatch latch) {
      this.clock = clock;
      this.eventCountLatch = latch;
      this.rand = new Random();
    }

    @Override
    public void onNext(final Alarm value) {
      final int duration = rand.nextInt(100) + 1;
      clock.scheduleAlarm(duration, this);
      eventCountLatch.countDown();
    }
  }
}
