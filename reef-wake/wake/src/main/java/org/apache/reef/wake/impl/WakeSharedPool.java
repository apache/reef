/**
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
package org.apache.reef.wake.impl;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.WakeParameters;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


// This implementation uses the fork join framework to reduce the cost of spawning
// events in stages. For two participating stages back to back, the pool allows
// for the thread in the first stage to execute the event it submits to the second stage.
// These choices are made by the ForkJoinPool.
// 
// So, this does sort of go against the reason for stages, but doesn't eliminate them
// and raises the level of abstraction that Wake sees above threads. 
//
// this will only be deadlock free if blocking synchronization done by events is safe.
// That is no event submitted to the pool can have a producer/consumer dependency
// on another event submitted to the pool
public class WakeSharedPool implements Stage {
  private static final Logger LOG = Logger.getLogger(WakeSharedPool.class.getName());

  // not a constant, so specify here
  private static final int DEFAULT_PARALLELISM = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
  private final ForkJoinPool pool;
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  private AtomicBoolean closed = new AtomicBoolean(false);

  @Inject
  public WakeSharedPool(@Parameter(Parallelism.class) int parallelism) {
    this.pool = new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // TODO: need to pass this upwards to REEF can grab it
      }
    },
        // async mode turned on so a task that invokes other tasks does not have to join on them.
        // this is appropriate for event-based tasks, where once you submit an event to a stage it
        // is always fire-and-forget.
        true);

    // register it with the StageManager, since the pool is meant to back stages
    StageManager.instance().register(this);
  }

  // TODO do we need this?
  //public ForkJoinPool pool() {
  //  return pool;
  //}

  @Inject
  public WakeSharedPool() {
    this(DEFAULT_PARALLELISM);
  }

  public void submit(ForkJoinTask<?> t) {
    if (ForkJoinTask.inForkJoinPool()) {
      ForkJoinTask.invokeAll(t);            // alternatively just pool().pool.execute(t), which simply forces it to be this pool (right now we expect only one anyway)
    } else {
      pool.submit(t);
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("ending pool stage: " + pool.toString());
    if (closed.compareAndSet(false, true)) {
      pool.shutdown();
      if (!pool.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        List<Runnable> droppedRunnables = pool.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

  @NamedParameter
  private static class Parallelism implements Name<Integer> {
  }
}
