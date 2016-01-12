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

  private static final Logger LOG = Logger.getLogger(Clock.class.toString());

  private final Timer timer;

  private final TreeSet<Time> schedule;

  private final PubSubEventHandler<Time> handlers;

  private final InjectionFuture<Set<EventHandler<StartTime>>> startHandler;
  private final InjectionFuture<Set<EventHandler<StopTime>>> stopHandler;
  private final InjectionFuture<Set<EventHandler<RuntimeStart>>> runtimeStartHandler;
  private final InjectionFuture<Set<EventHandler<RuntimeStop>>> runtimeStopHandler;
  private final InjectionFuture<Set<EventHandler<IdleClock>>> idleHandler;

  private Throwable stoppedOnException;
  private boolean closed = false;

  @Inject
  RuntimeClock(final Timer timer,
               @Parameter(Clock.StartHandler.class) final InjectionFuture<Set<EventHandler<StartTime>>> startHandler,
               @Parameter(StopHandler.class) final InjectionFuture<Set<EventHandler<StopTime>>> stopHandler,
               @Parameter(Clock.RuntimeStartHandler.class)
               final InjectionFuture<Set<EventHandler<RuntimeStart>>> runtimeStartHandler,
               @Parameter(Clock.RuntimeStopHandler.class)
               final InjectionFuture<Set<EventHandler<RuntimeStop>>> runtimeStopHandler,
               @Parameter(IdleHandler.class) final InjectionFuture<Set<EventHandler<IdleClock>>> idleHandler) {
    this.timer = timer;
    this.schedule = new TreeSet<>();
    this.handlers = new PubSubEventHandler<>();

    this.startHandler = startHandler;
    this.stopHandler = stopHandler;
    this.runtimeStartHandler = runtimeStartHandler;
    this.runtimeStopHandler = runtimeStopHandler;
    this.idleHandler = idleHandler;

    this.stoppedOnException = null;

    LOG.log(Level.FINE, "RuntimeClock instantiated.");
  }

  @Override
  public void scheduleAlarm(final int offset, final EventHandler<Alarm> handler) {
    synchronized (this.schedule) {
      if (this.closed) {
        throw new IllegalStateException("Scheduling alarm on a closed clock");
      }

      this.schedule.add(new ClientAlarm(this.timer.getCurrent() + offset, handler));
      this.schedule.notifyAll();
    }
  }

  public void registerEventHandler(final Class<? extends Time> clazz, final EventHandler<Time> handler) {
    this.handlers.subscribe(clazz, handler);
  }

  public void scheduleRuntimeAlarm(final int offset, final EventHandler<Alarm> handler) {
    synchronized (this.schedule) {
      this.schedule.add(new RuntimeAlarm(this.timer.getCurrent() + offset, handler));
      this.schedule.notifyAll();
    }
  }

  @Override
  public void stop() {
    this.stop(null);
  }

  @Override
  public void stop(final Throwable stopOnException) {
    LOG.entering(RuntimeClock.class.getCanonicalName(), "stop");
    synchronized (this.schedule) {
      this.schedule.clear();
      this.schedule.add(new StopTime(timer.getCurrent()));
      this.schedule.notifyAll();
      this.closed = true;
      if (this.stoppedOnException == null) {
        this.stoppedOnException = stopOnException;
      }
    }
    LOG.exiting(RuntimeClock.class.getCanonicalName(), "stop");
  }

  @Override
  public void close() {
    LOG.entering(RuntimeClock.class.getCanonicalName(), "close");
    synchronized (this.schedule) {
      if (this.closed) {
        LOG.log(Level.INFO, "Clock is already closed");
        return;
      }
      this.schedule.clear();
      this.schedule.add(new StopTime(findAcceptableStopTime()));
      this.schedule.notifyAll();
      this.closed = true;
      LOG.log(Level.INFO, "Clock.close()");
    }
    LOG.exiting(RuntimeClock.class.getCanonicalName(), "close");
  }

  /**
   * Finds an acceptable stop time, which is the
   * a time beyond that of any client alarm.
   *
   * @return an acceptable stop time
   */
  private long findAcceptableStopTime() {
    long time = timer.getCurrent();
    for (final Time t : this.schedule) {
      if (t instanceof ClientAlarm) {
        assert time <= t.getTimeStamp();
        time = t.getTimeStamp();
      }
    }
    return time + 1;
  }


  @Override
  public boolean isIdle() {
    synchronized (this.schedule) {
      for (final Time t : this.schedule) {
        if (t instanceof ClientAlarm) {
          return false;
        }
      }
      return true;
    }
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  private <T extends Time> void subscribe(final Class<T> eventClass, final Set<EventHandler<T>> handlers) {
    for (final EventHandler<T> handler : handlers) {
      this.handlers.subscribe(eventClass, handler);
    }
  }

  /**
   * Logs the currently running threads.
   *
   * @param level  the level used for the log entry
   * @param prefix put before the comma-separated list of threads
   */
  private void logThreads(final Level level, final String prefix) {
    final StringBuilder sb = new StringBuilder(prefix);
    for (final Thread t : Thread.getAllStackTraces().keySet()) {
      sb.append(t.getName());
      sb.append(", ");
    }
    LOG.log(level, sb.toString());
  }

  @Override
  public void run() {
    LOG.entering(RuntimeClock.class.getCanonicalName(), "run");

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
      final StartTime start = new StartTime(this.timer.getCurrent());
      this.handlers.onNext(start);

      while (true) {
        LOG.log(Level.FINEST, "Entering clock main loop iteration.");
        try {
          if (this.isIdle()) {
            // Handle an idle clock event, without locking this.schedule
            this.handlers.onNext(new IdleClock(timer.getCurrent()));
          }

          Time time = null;
          synchronized (this.schedule) {
            while (this.schedule.isEmpty()) {
              this.schedule.wait();
            }

            assert this.schedule.first() != null;

            // Wait until the first scheduled time is ready
            for (long duration = this.timer.getDuration(this.schedule.first().getTimeStamp());
                 duration > 0;
                 duration = this.timer.getDuration(this.schedule.first().getTimeStamp())) {
              // note: while I'm waiting, another alarm could be scheduled with a shorter duration
              // so the next time I go around the loop I need to revise my duration
              this.schedule.wait(duration);
            }
            // Remove the event from the schedule and process it:
            time = this.schedule.pollFirst();
            assert time != null;
          }

          if (time instanceof Alarm) {
            final Alarm alarm = (Alarm) time;
            alarm.handle();
          } else {
            this.handlers.onNext(time);
            if (time instanceof StopTime) {
              break; // we're done.
            }
          }
        } catch (final InterruptedException expected) {
          // waiting interrupted - return to loop
        }
      }
      if (this.stoppedOnException == null) {
        this.handlers.onNext(new RuntimeStop(this.timer.getCurrent()));
      } else {
        this.handlers.onNext(new RuntimeStop(this.timer.getCurrent(), this.stoppedOnException));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      this.handlers.onNext(new RuntimeStop(this.timer.getCurrent(), e));
    } finally {
      logThreads(Level.FINE, "Threads running after exiting the clock main loop: ");
      LOG.log(Level.FINE, "Runtime clock exit");
    }
    LOG.exiting(RuntimeClock.class.getCanonicalName(), "run");

  }


}
