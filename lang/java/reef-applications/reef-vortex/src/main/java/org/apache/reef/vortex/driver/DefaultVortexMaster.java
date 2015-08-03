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
package org.apache.reef.vortex.driver;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of VortexMaster.
 * It uses two thread-safe data structures(pendingTasklets, runningWorkers) in implementing VortexMaster interface.
 * It also runs a dedicated thread for scheduling/launching pending tasklets.
 */
@ThreadSafe
@DriverSide
final class DefaultVortexMaster implements VortexMaster {
  private static final Logger LOG = Logger.getLogger(DefaultVortexMaster.class.getName());
  private final AtomicInteger taskletIdCounter = new AtomicInteger();
  private final RunningWorkers runningWorkers;
  private final BlockingDeque<Tasklet> pendingTasklets = new LinkedBlockingDeque<>();
  private final ExecutorService schedulerThread = Executors.newSingleThreadExecutor();

  /**
   * @param runningWorkers for managing all running workers.
   */
  @Inject
  DefaultVortexMaster(final RunningWorkers runningWorkers) {
    this.runningWorkers = runningWorkers;
    schedulerThread.execute(new Runnable() {
      @Override
      public void run() {
        while (!runningWorkers.isTerminated()) {
          try {
            final Tasklet tasklet = pendingTasklets.takeFirst(); // blocks when no tasklet exists
            runningWorkers.launchTasklet(tasklet); // blocks when no worker exists
          } catch (InterruptedException e) {
            LOG.log(Level.INFO, "Interrupted upon termination");
          }
        }
      }
    });
  }

  /**
   * Add a new tasklet to pendingTasklets.
   */
  @Override
  public <TInput extends Serializable, TOutput extends Serializable> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> function, final TInput input) {
    // TODO[REEF-500]: Simple duplicate Vortex Tasklet launch.
    final VortexFuture<TOutput> vortexFuture = new VortexFuture<>();
    this.pendingTasklets.addLast(new Tasklet<>(taskletIdCounter.getAndIncrement(), function, input, vortexFuture));
    return vortexFuture;
  }

  /**
   * Add a new worker to runningWorkers.
   */
  @Override
  public void workerAllocated(final VortexWorkerManager vortexWorkerManager) {
    runningWorkers.addWorker(vortexWorkerManager);
  }

  /**
   * Remove the worker from runningWorkers and add back the lost tasklets to pendingTasklets.
   */
  @Override
  public void workerPreempted(final String id) {
    final Collection<Tasklet> preemptedTasklets = runningWorkers.removeWorker(id);
    for (final Tasklet tasklet : preemptedTasklets) {
      pendingTasklets.addFirst(tasklet);
    }
  }

  /**
   * Notify task completion to runningWorkers.
   */
  @Override
  public void taskletCompleted(final String workerId,
                               final int taskletId,
                               final Serializable result) {
    runningWorkers.completeTasklet(workerId, taskletId, result);
  }

  /**
   * Notify task failure to runningWorkers.
   */
  @Override
  public void taskletErrored(final String workerId, final int taskletId, final Exception exception) {
    runningWorkers.errorTasklet(workerId, taskletId, exception);
  }

  /**
   * Terminate the job.
   */
  @Override
  public void terminate() {
    runningWorkers.terminate();
    schedulerThread.shutdownNow();
  }

  /**
   * For unit tests only.
   */
  VortexFuture enqueueMockedTasklet(final Tasklet tasklet) {
    final VortexFuture vortexFuture = new VortexFuture<>();
    this.pendingTasklets.addLast(tasklet);
    return vortexFuture;
  }
}
