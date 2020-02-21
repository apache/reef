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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingUtils;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.test.time.util.AlarmProducer;
import org.apache.reef.wake.test.time.util.EventRecorder;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.LogicalTimer;
import org.apache.reef.wake.time.runtime.RealTimer;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Tests for RuntimeClock event loop.
 */
public class RuntimeClockTest {

  private static final Tang TANG = Tang.Factory.getTang();

  private final Random rand = new Random();

  /**
   * Create new RuntimeClock object injected with the given timer.
   *
   * @param timerClass Timer to use inside the RuntimeClock. Must implement the Timer interface.
   * @return A new instance of the RuntimeClock, instrumented with the given timer.
   * @throws InjectionException On configuration error.
   */
  private static RuntimeClock buildClock(
      final Class<? extends Timer> timerClass) throws InjectionException {

    final Configuration clockConfig = TANG.newConfigurationBuilder()
        .bind(Timer.class, timerClass)
        .build();

    return TANG.newInjector(clockConfig).getInstance(RuntimeClock.class);
  }

  /**
   * Create 10 threads to produce 40 alarms at random intervals
   * and check if all alarms get processed.
   * @throws Exception ThreadPoolStage can throw anything.
   */
  @Test
  public void testClock() throws Exception {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    try (RuntimeClock clock = buildClock(RealTimer.class)) {

      new Thread(clock).start();

      final CountDownLatch eventCountLatch = new CountDownLatch(40);
      final AlarmProducer alarmProducer = new AlarmProducer(clock, eventCountLatch) {
        @Override
        public int getOffset() {
          return randomOffsetUniform(rand, 1, 100);
        }
      };

      try (ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(alarmProducer, 10)) {
        stage.onNext(null);
        Assert.assertTrue(eventCountLatch.await(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void testAlarmRegistrationRaceConditions() throws Exception {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    try (RuntimeClock clock = buildClock(RealTimer.class)) {

      new Thread(clock).start();

      final EventRecorder earlierAlarmRecorder = new EventRecorder();
      final EventRecorder laterAlarmRecorder = new EventRecorder();

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
    }
  }

  @Test
  public void testMultipleCloseCalls() throws Exception {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    final int numThreads = 3;
    final CountDownLatch eventCountLatch = new CountDownLatch(numThreads);

    try (RuntimeClock clock = buildClock(RealTimer.class)) {

      final EventHandler<Alarm> handler = new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm value) {
          clock.close();
          eventCountLatch.countDown();
        }
      };

      new Thread(clock).start();

      try (ThreadPoolStage<Alarm> stage = new ThreadPoolStage<>(handler, numThreads)) {

        for (int i = 0; i < numThreads; ++i) {
          stage.onNext(null);
        }

        Assert.assertTrue(eventCountLatch.await(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void testSimultaneousAlarms() throws Exception {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    final int expectedEvent = 2;
    final CountDownLatch eventCountLatch = new CountDownLatch(expectedEvent);

    try (RuntimeClock clock = buildClock(LogicalTimer.class)) {

      new Thread(clock).start();

      final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);

      clock.scheduleAlarm(500, alarmRecorder);
      clock.scheduleAlarm(500, alarmRecorder);

      eventCountLatch.await(10, TimeUnit.SECONDS);

      Assert.assertEquals(expectedEvent, alarmRecorder.getEventCount());
    }
  }

  @Test
  public void testAlarmOrder() throws Exception {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    final int numAlarms = 10;
    final CountDownLatch eventCountLatch = new CountDownLatch(numAlarms);
    final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);

    final long[] expected = new long[numAlarms];

    try (RuntimeClock clock = buildClock(RealTimer.class)) {

      new Thread(clock).start();

      for (int i = 0; i < numAlarms; ++i) {
        final Time event = clock.scheduleAlarm(i * 100, alarmRecorder);
        expected[i] = event.getTimestamp();
      }
    }

    eventCountLatch.await(10, TimeUnit.SECONDS);

    int i = 0;
    final long[] actual = new long[numAlarms];
    for (final Time event : alarmRecorder.getEvents()) {
      actual[i++] = event.getTimestamp();
    }

    Assert.assertEquals(
        "Number of alarms does not match the expected count",
        numAlarms, alarmRecorder.getEventCount());

    Assert.assertArrayEquals("Alarms processed in the wrong order", expected, actual);
  }

  /**
   * Test graceful shutdown of the event loop.
   * Schedule two events and close the clock. Make sure that no events occur soon after
   * closing the alarm and both of them occur at the scheduled time. Check that the clock
   * is closed after that.
   * @throws InjectionException Error building a runtime clock object.
   * @throws InterruptedException Sleep interrupted.
   */
  @Test
  public void testGracefulClose() throws InjectionException, InterruptedException {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    final int numAlarms = 2;
    final CountDownLatch eventCountLatch = new CountDownLatch(numAlarms);
    final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);

    final RuntimeClock clock = buildClock(RealTimer.class);
    new Thread(clock).start();

    clock.scheduleAlarm(100, alarmRecorder);
    clock.scheduleAlarm(101, alarmRecorder);
    clock.close();

    Assert.assertFalse("Clock cannot be idle yet", clock.isIdle());
    Assert.assertTrue("Clock must be in closed state", clock.isClosed());

    Thread.sleep(10);
    Assert.assertTrue(
        "No events should occur immediately after the graceful shutdown",
        alarmRecorder.getEvents().isEmpty());

    Thread.sleep(200);
    final List<Time> events = alarmRecorder.getEvents();
    Assert.assertEquals("Expected events on graceful shutdown", events.size(), numAlarms);

    Assert.assertTrue("No client alarms should be scheduled at this time", clock.isIdle());
    Assert.assertTrue("Clock must be in closed state", clock.isClosed());
  }

  /**
   * Test forceful shutdown of the event loop. Schedule two events and close the clock.
   * Make sure that no events occur after that and the clock is in closed and idle state.
   * @throws InjectionException Error building a runtime clock object.
   * @throws InterruptedException Sleep interrupted.
   */
  @Test
  public void testForcefulStop() throws InjectionException, InterruptedException {

    LoggingUtils.setLoggingLevel(Level.FINEST);

    final int numAlarms = 2;
    final CountDownLatch eventCountLatch = new CountDownLatch(numAlarms);
    final EventRecorder alarmRecorder = new EventRecorder(eventCountLatch);

    final RuntimeClock clock = buildClock(RealTimer.class);
    new Thread(clock).start();

    clock.scheduleAlarm(100, alarmRecorder);
    clock.scheduleAlarm(101, alarmRecorder);
    clock.stop();

    Assert.assertTrue("Clock must be idle already", clock.isIdle());
    Assert.assertTrue("Clock must be in closed state", clock.isClosed());

    Thread.sleep(200);
    Assert.assertTrue("No events should be in the schedule", alarmRecorder.getEvents().isEmpty());
  }
}
