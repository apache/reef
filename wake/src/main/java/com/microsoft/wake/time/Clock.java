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
package com.microsoft.wake.time;


import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.MissingStartHandlerHandler;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.IdleClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

import java.util.Set;

/**
 * Represents a clock.
 */
@DefaultImplementation(RuntimeClock.class)
public interface Clock extends Runnable, AutoCloseable {

  /**
   * Bind this to an event handler to statically subscribe to the StartTime Event
   */
  @NamedParameter(default_class = MissingStartHandlerHandler.class, doc = "Will be called upon the start event")
  public class StartHandler implements Name<Set<EventHandler<StartTime>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the StopTime Event
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the stop event")
  public class StopHandler implements Name<Set<EventHandler<StopTime>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the RuntimeStart Event
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the runtime start event")
  public class RuntimeStartHandler implements Name<Set<EventHandler<RuntimeStart>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the RuntimeStart Event
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the runtime stop event")
  public class RuntimeStopHandler implements Name<Set<EventHandler<RuntimeStop>>> {
  }

  /**
   * Bind this to an event handler to statically subscribe to the IdleClock Event
   */
  @NamedParameter(default_class = LoggingEventHandler.class, doc = "Will be called upon the Idle event")
  public class IdleHandler implements Name<Set<EventHandler<IdleClock>>> {
  }

  /**
   * Schedule a TimerEvent at the given future offset
   *
   * @param handler to be called
   * @param offset  into the future
   * @throws IllegalStateException when the clock has been already closed
   */
  public void scheduleAlarm(final int offset, final EventHandler<Alarm> handler);

  /**
   * This will stop the clock after all client alarms
   * finish executing.
   */
  @Override
  public void close();


  /**
   * This stops the clock immediately, without waiting for
   * client alarms to finish.
   */
  public void stop();

  /**
   * Clock is idle if it has no future Alarms set.
   *
   * @return true if idle, otherwise false
   */
  public boolean isIdle();
}
