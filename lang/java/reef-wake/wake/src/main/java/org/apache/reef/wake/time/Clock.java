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
package org.apache.reef.wake.time;


import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.MissingStartHandlerHandler;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.event.IdleClock;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import java.util.Set;

/**
 * Represents a clock.
 */
@DefaultImplementation(RuntimeClock.class)
public interface Clock extends Runnable, AutoCloseable {

  /**
   * Schedule a TimerEvent at the given future offset.
   * @param handler Event handler to be called on alarm.
   * @param offset Offset into the future in milliseconds.
   * @return Newly scheduled alarm.
   * @throws IllegalStateException When the clock has been already closed.
   */
  Time scheduleAlarm(final int offset, final EventHandler<Alarm> handler);

  /**
   * This will stop the clock after all client alarms
   * finish executing.
   */
  @Override
  void close();

  /**
   * This stops the clock immediately, without waiting for
   * client alarms to finish.
   */
  void stop();

  /**
   * This stops the clock immediately, without waiting for
   * client alarms to finish. Stops with an exception that
   * is propagated to RuntimeStopHandlers.
   */
  void stop(final Throwable exception);

  /**
   * Clock is idle if it has no future Alarms set.
   *
   * @return true if idle, otherwise false
   */
  boolean isIdle();

  /**
   * Clock is closed after a call to stop() or close().
   * A closed clock cannot add new alarms to the schedule, but, in case of the
   * graceful shutdown, can still invoke previously scheduled ones.
   * @return true if closed, false otherwise.
   */
  boolean isClosed();

  /**
   * Bind this to an event handler to statically subscribe to the StartTime Event.
   */
  @NamedParameter(default_class = MissingStartHandlerHandler.class, doc = "Will be called upon the start event")
  class StartHandler implements Name<Set<EventHandler<StartTime>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the StopTime Event.
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the stop event")
  class StopHandler implements Name<Set<EventHandler<StopTime>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the RuntimeStart Event.
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the runtime start event")
  class RuntimeStartHandler implements Name<Set<EventHandler<RuntimeStart>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the RuntimeStart Event.
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the runtime stop event")
  class RuntimeStopHandler implements Name<Set<EventHandler<RuntimeStop>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the IdleClock Event.
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the Idle event")
  class IdleHandler implements Name<Set<EventHandler<IdleClock>>> {
  }
}
