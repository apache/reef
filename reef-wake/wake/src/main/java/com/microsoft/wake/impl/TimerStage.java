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
package com.microsoft.wake.impl;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;


import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Stage;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.StageConfiguration.StageHandler;
import com.microsoft.wake.StageConfiguration.StageName;
import com.microsoft.wake.StageConfiguration.TimerInitialDelay;
import com.microsoft.wake.StageConfiguration.TimerPeriod;

/**
 * Stage that triggers an event handler periodically
 */
public final class TimerStage implements Stage {
  private static final Logger LOG = Logger.getLogger(TimerStage.class.getName());

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ScheduledExecutorService executor;
  private final PeriodicEvent event = new PeriodicEvent();
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  
  /**
   * Constructs a timer stage with no initial delay
   * 
   * @param handler an event handler
   * @param period a period in milli-seconds
   */
  @Inject
  public TimerStage(@Parameter(StageHandler.class) final EventHandler<PeriodicEvent> handler, 
      @Parameter(TimerPeriod.class) final long period) {
    this(handler, 0, period);
  }
  
  /**
   * Constructs a timer stage with no initial delay
   * 
   * @name name the stage name
   * @param handler an event handler
   * @param period a period in milli-seconds
   */
  @Inject
  public TimerStage(@Parameter(StageName.class) final String name, 
      @Parameter(StageHandler.class) final EventHandler<PeriodicEvent> handler,
      @Parameter(TimerPeriod.class) final long period) {
    this(name, handler, 0, period);
  }
  
  /**
   * Constructs a timer stage
   * 
   * @param handler an event handler
   * @param initialDelay an initial delay 
   * @param period a period in milli-seconds
   */
  @Inject
  public TimerStage(@Parameter(StageHandler.class) final EventHandler<PeriodicEvent> handler, 
      @Parameter(TimerInitialDelay.class) final long initialDelay, 
      @Parameter(TimerPeriod.class) final long period) {
    this(handler.getClass().getName(), handler, initialDelay, period);
  }
  
  /**
   * Constructs a timer stage
   * 
   * @param name the stage name
   * @param handler an event handler
   * @param initialDelay an initial delay 
   * @param period a period in milli-seconds
   */
  @Inject
  public TimerStage(@Parameter(StageName.class) final String name, 
      @Parameter(StageHandler.class) final EventHandler<PeriodicEvent> handler,
      @Parameter(TimerInitialDelay.class) final long initialDelay,
      @Parameter(TimerPeriod.class) final long period) {
    this.executor = Executors.newScheduledThreadPool(1, new DefaultThreadFactory(name));
    executor.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        if (LOG.isLoggable(Level.FINEST)) LOG.log(Level.FINEST, "{0} {1}", new Object[] {name, event});
        handler.onNext(event);
      }
      
    }, initialDelay, period, TimeUnit.MILLISECONDS);
    StageManager.instance().register(this);
  }

	
  /**
   * Closes resources
   * 
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown();
      if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

}

