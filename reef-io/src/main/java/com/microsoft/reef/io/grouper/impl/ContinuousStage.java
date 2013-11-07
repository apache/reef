/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io.grouper.impl;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.AbstractEStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.exception.WakeRuntimeException;
import com.microsoft.wake.impl.StageManager;
import com.microsoft.wake.rx.Observer;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;


/**
 * Stage that executes the observer with a thread pool
 *
 * @param <T> type
 */
public final class ContinuousStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(ContinuousStage.class.getName());

  private final Observer<Integer> handler;
  private final AtomicReference<Thread[]> threads;
  private final boolean[] done;
  private final int numThreads;
  private final AtomicBoolean closed;
  private final AtomicInteger active;

  private final String stageName;
  
  private final long period_ms;
  private final int period_ns;
  
  @NamedParameter(default_value="0")
  public final static class PeriodNS implements Name<Long>{}
  
  /**
   * Constructs a stage that continuously executes an event with specified number of threads
   *
   * @param observer   the observer to execute
   * @param numThreads the number of threads
   */
  @Inject
  public ContinuousStage(@Parameter(StageConfiguration.StageObserver.class) Observer<Integer> handler, @Parameter(StageConfiguration.NumberOfThreads.class) int numThreads) {
    this(handler, numThreads, "ContinuousStage-"+handler.getClass().getName());
  }
  
  @Inject
  public ContinuousStage(@Parameter(StageConfiguration.StageObserver.class) Observer<Integer> handler, @Parameter(StageConfiguration.NumberOfThreads.class) int numThreads, @Parameter(StageConfiguration.StageName.class) String name) {
    this(handler, numThreads, name, 0);
  }
  
  @Inject
  public ContinuousStage(@Parameter(StageConfiguration.StageObserver.class) Observer<Integer> handler,
                         @Parameter(StageConfiguration.NumberOfThreads.class) int numThreads, 
                         @Parameter(StageConfiguration.StageName.class) String name,
                         @Parameter(PeriodNS.class) long period_ns) {
    super(name);
    this.stageName = name;
    this.handler = handler;
    if (numThreads <= 0)
      throw new WakeRuntimeException("numThreads " + numThreads + " is less than or equal to 0");

    this.threads = new AtomicReference<>(null);

    this.done = new boolean[numThreads];
    Arrays.fill(this.done, false);
    this.numThreads = numThreads;
    this.closed = new AtomicBoolean(false);

    this.active = new AtomicInteger();
    
    // put the period into the format needed by Thread.sleep
    this.period_ms = Math.max(0L, (period_ns-999999)/(1000*1000));
    this.period_ns = (int)Math.max(0L, period_ns-(this.period_ms*1000*1000));

    StageManager.instance().register(this);
  }

  /**
   * Starts the stage with new continuous event
   *
   * @param value the new value
   */
  @Override
  public void onNext(final T value) {
    beforeOnNext();
    // first call starts the stage; subsequent are ignored
    if (!this.threads.compareAndSet(null, new Thread[numThreads])) {
      return;
    }


    active.set(numThreads);
    Thread[] cthreads = threads.get();
    for (int i = 0; i < numThreads; i++) {
      final int ci = i;
      cthreads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            while(!done[ci]) {
              Thread.sleep(period_ms, period_ns);
              handler.onNext(ci);
            } 
          } catch (InterruptedException e) {
            LOG.severe(e.toString());
            e.printStackTrace();
          } catch (Exception e) { // TODO: temporary interposition
            e.printStackTrace();
            throw e;
          } finally {
            LOG.fine("Continuous thread exits");
          }
        }
      }, stageName+"-"+i);
      cthreads[i].start();
    }
    afterOnNext();
  }


  /**
   * Closes the stage
   *
   * @return Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      Thread[] threadsArr = threads.get();
      if (threadsArr == null) {
        LOG.warning("close() was called without onNext");
      } else {
        for (Thread t : threadsArr) {
          t.join();
        }
      }
    } else {
      LOG.warning("close() was already called. This call is ignored");
    }
  }

  public EventHandler<Integer> getDoneHandler() {
    return new EventHandler<Integer>() {
      @Override
      public void onNext(Integer id) {
        done[id] = true;
        if (0 == active.decrementAndGet()) {
          handler.onCompleted();
        }
      }
    };
  }

}
