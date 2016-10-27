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
package org.apache.reef.wake.time.runtime;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.PubSubEventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.wake.time.runtime.event.*;

import javax.inject.Inject;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of clock.
 *
 * After invoking `RuntimeStart` and `StartTime` events initially,
 * this invokes scheduled events on time. If there is no scheduled event,
 * `IdleClock` event is invoked.
 */
public final class RuntimeClock implements Clock {

  private static final Logger LOG = Logger.getLogger(RuntimeClock.class.getName());
  private static final String CLASS_NAME = RuntimeClock.class.getCanonicalName();

  /**
   * Injectable source of current time information.
   * Usually an instance of RealTimer that wraps the system clock.
   */
  private final Timer timer;

  /**
   * An ordered set of timed objects, in ascending order of their timestamps.
   * It also serves as the main synchronization monitor for the class.
   */
  private final TreeSet<Time> schedule = new TreeSet<>();

  /** Event handlers - populated with the injectable parameters provided to the RuntimeClock constructor. */
  private final PubSubEventHandler<Time> handlers = new PubSubEventHandler<>();

  private final InjectionFuture<Set<EventHandler<StartTime>>> startHandler;
  private final InjectionFuture<Set<EventHandler<StopTime>>> stopHandler;
  private final InjectionFuture<Set<EventHandler<RuntimeStart>>> runtimeStartHandler;
  private final InjectionFuture<Set<EventHandler<RuntimeStop>>> runtimeStopHandler;
  private final InjectionFuture<Set<EventHandler<IdleClock>>> idleHandler;

  /**
   * Timestamp of the last client alarm in the schedule.
   * We use it to schedule a graceful shutdown event immediately after all client alarms.
   */
  private long lastClientAlarm = 0;

  /**
   * Number of client alarms in the schedule.
   * We need it to determine whether event loop is idle (i.e. has no client alarms scheduled)
   */
  private int numClientAlarms = 0;

  /** Set to true when the clock is closed. */
  private boolean isClosed = false;

  /** Exception that caused the clock to stop. */
  private Throwable exceptionCausedStop = null;

  @Inject
  private RuntimeClock(
      final Timer timer,
      @Parameter(Clock.StartHandler.class)
          final InjectionFuture<Set<EventHandler<StartTime>>> startHandler,
      @Parameter(Clock.StopHandler.class)
          final InjectionFuture<Set<EventHandler<StopTime>>> stopHandler,
      @Parameter(Clock.RuntimeStartHandler.class)
          final InjectionFuture<Set<EventHandler<RuntimeStart>>> runtimeStartHandler,
      @Parameter(Clock.RuntimeStopHandler.class)
          final InjectionFuture<Set<EventHandler<RuntimeStop>>> runtimeStopHandler,
      @Parameter(Clock.IdleHandler.class)
          final InjectionFuture<Set<EventHandler<IdleClock>>> idleHandler) {

    this.timer = timer;
    this.startHandler = startHandler;
    this.stopHandler = stopHandler;
    this.runtimeStartHandler = runtimeStartHandler;
    this.runtimeStopHandler = runtimeStopHandler;
    this.idleHandler = idleHandler;

    LOG.log(Level.FINE, "RuntimeClock instantiated.");
  }

  /**
   * Schedule a new Alarm event in `offset` milliseconds into the future,
   * and supply an event handler to be called at that time.
   * @param offset Number of milliseconds into the future relative to current time.
   * @param handler Event handler to be invoked.
   * @return Newly scheduled alarm.
   * @throws IllegalStateException if the clock is already closed.
   */
  @Override
  public Time scheduleAlarm(final int offset, final EventHandler<Alarm> handler) {

    final Time alarm = new ClientAlarm(this.timer.getCurrent() + offset, handler);

    if (LOG.isLoggable(Level.FINEST)) {

      final int eventQueueLen;
      synchronized (this.schedule) {
        eventQueueLen = this.numClientAlarms;
      }

      LOG.log(Level.FINEST,
          "Schedule alarm: {0} Outstanding client alarms: {1}",
          new Object[] {alarm, eventQueueLen});
    }

    synchronized (this.schedule) {

      if (this.isClosed) {
        throw new IllegalStateException("Scheduling alarm on a closed clock");
      }

      if (alarm.getTimestamp() > this.lastClientAlarm) {
        this.lastClientAlarm = alarm.getTimestamp();
      }

      assert this.numClientAlarms >= 0;
      ++this.numClientAlarms;

      this.schedule.add(alarm);
      this.schedule.notify();
    }

    return alarm;
  }

  /**
   * Stop the clock. Remove all other events from the schedule and fire StopTimer
   * event immediately. It is recommended to use close() method for graceful shutdown
   * instead of stop().
   */
  @Override
  public void stop() {
    this.stop(null);
  }

  /**
   * Stop the clock on exception.
   * Remove all other events from the schedule and fire StopTimer event immediately.
   * @param exception Exception that is the cause for the stop. Can be null.
   */
  @Override
  public void stop(final Throwable exception) {

    LOG.entering(CLASS_NAME, "stop");

    synchronized (this.schedule) {

      if (this.isClosed) {
        LOG.log(Level.FINEST, "Clock has already been closed");
        return;
      }

      this.isClosed = true;
      this.exceptionCausedStop = exception;

      final Time stopEvent = new StopTime(this.timer.getCurrent());
      LOG.log(Level.FINE,
          "Stop scheduled immediately: {0} Outstanding client alarms: {1}",
          new Object[] {stopEvent, this.numClientAlarms});

      assert this.numClientAlarms >= 0;
      this.numClientAlarms = 0;

      this.schedule.clear();
      this.schedule.add(stopEvent);
      this.schedule.notify();
    }

    LOG.exiting(CLASS_NAME, "stop");
  }

  /**
   * Wait for all client alarms to finish executing and gracefully shutdown the clock.
   */
  @Override
  public void close() {

    LOG.entering(CLASS_NAME, "close");

    synchronized (this.schedule) {

      if (this.isClosed) {
        LOG.exiting(CLASS_NAME, "close", "Clock has already been closed");
        return;
      }

      this.isClosed = true;

      final Time stopEvent = new StopTime(Math.max(this.timer.getCurrent(), this.lastClientAlarm + 1));
      LOG.log(Level.FINE,
          "Graceful shutdown scheduled: {0} Outstanding client alarms: {1}",
          new Object[] {stopEvent, this.numClientAlarms});

      this.schedule.add(stopEvent);
      this.schedule.notify();
    }

    LOG.exiting(CLASS_NAME, "close");
  }

  /**
   * Check if there are no client alarms scheduled.
   * @return True if there are no client alarms in the schedule, false otherwise.
   */
  @Override
  public boolean isIdle() {
    synchronized (this.schedule) {
      assert this.numClientAlarms >= 0;
      return this.numClientAlarms == 0;
    }
  }

  /**
   * The clock is closed after a call to stop() or close().
   * A closed clock cannot add new alarms to the schedule, but, in case of the
   * graceful shutdown, can still invoke previously scheduled ones.
   * @return true if closed, false otherwise.
   */
  @Override
  public boolean isClosed() {
    synchronized (this.schedule) {
      return this.isClosed;
    }
  }

  /**
   * Register event handlers for the given event class.
   * @param eventClass Event type to handle. Must be derived from Time.
   * @param handlers One or many event handlers that can process given event type.
   * @param <T> Event type - must be derived from class Time. (i.e. contain a timestamp).
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  private <T extends Time> void subscribe(final Class<T> eventClass, final Set<EventHandler<T>> handlers) {
    for (final EventHandler<T> handler : handlers) {
      LOG.log(Level.FINEST, "Subscribe: event {0} handler {1}", new Object[] {eventClass.getName(), handler});
      this.handlers.subscribe(eventClass, handler);
    }
  }

  /**
   * Main event loop.
   * Set up the event handlers, and go into event loop that polls the schedule and process events in it.
   */
  @Override
  public void run() {

    LOG.entering(CLASS_NAME, "run");

    try {

      LOG.log(Level.FINE, "Subscribe event handlers");

      subscribe(StartTime.class, this.startHandler.get());
      subscribe(StopTime.class, this.stopHandler.get());
      subscribe(RuntimeStart.class, this.runtimeStartHandler.get());
      subscribe(RuntimeStop.class, this.runtimeStopHandler.get());
      subscribe(IdleClock.class, this.idleHandler.get());

      LOG.log(Level.FINE, "Initiate runtime start");
      this.handlers.onNext(new RuntimeStart(this.timer.getCurrent()));

      LOG.log(Level.FINE, "Initiate start time");
      this.handlers.onNext(new StartTime(this.timer.getCurrent()));

      while (true) {

        LOG.log(Level.FINEST, "Enter clock main loop.");

        try {

          if (this.isIdle()) {
            // Handle an idle clock event, without locking this.schedule
            this.handlers.onNext(new IdleClock(this.timer.getCurrent()));
          }

          final Time event;
          final int eventQueueLen;
          synchronized (this.schedule) {

            while (this.schedule.isEmpty()) {
              this.schedule.wait();
            }

            assert this.schedule.first() != null;

            // Wait until the first scheduled time is ready.
            // NOTE: while waiting, another alarm could be scheduled with a shorter duration
            // so the next time I go around the loop I need to revise my duration.
            while (true) {
              final long waitDuration = this.timer.getDuration(this.schedule.first());
              if (waitDuration <= 0) {
                break;
              }
              this.schedule.wait(waitDuration);
            }

            // Remove the event from the schedule and process it:
            event = this.schedule.pollFirst();

            if (event instanceof ClientAlarm) {
              --this.numClientAlarms;
              assert this.numClientAlarms >= 0;
            }

            eventQueueLen = this.numClientAlarms;
          }

          assert event != null;

          LOG.log(Level.FINER,
              "Process event: {0} Outstanding client alarms: {1}", new Object[] {event, eventQueueLen});

          if (event instanceof Alarm) {
            ((Alarm) event).run();
          } else {
            this.handlers.onNext(event);
            if (event instanceof StopTime) {
              break; // we're done.
            }
          }

        } catch (final InterruptedException expected) {
          LOG.log(Level.FINEST, "Wait interrupted; continue event loop.");
        }
      }

      this.handlers.onNext(new RuntimeStop(this.timer.getCurrent(), this.exceptionCausedStop));

    } catch (final Exception e) {

      LOG.log(Level.SEVERE, "Error in runtime clock", e);
      this.handlers.onNext(new RuntimeStop(this.timer.getCurrent(), e));

    } finally {
      LOG.log(Level.FINE, "Runtime clock exit");
    }

    LOG.exiting(CLASS_NAME, "run");
  }
}
