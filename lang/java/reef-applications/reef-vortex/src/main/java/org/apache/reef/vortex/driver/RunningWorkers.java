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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps track of all running VortexWorkers and Tasklets.
 * Upon Tasklet launch request, randomly schedules it to a VortexWorkerManager.
 */
@ThreadSafe
@DriverSide
final class RunningWorkers {
  private static final Logger LOG = Logger.getLogger(RunningWorkers.class.getName());

  // RunningWorkers and its locks
  private final HashMap<String, VortexWorkerManager> runningWorkers = new HashMap<>(); // Running workers/tasklets
  private final Set<Integer> taskletsToCancel = new HashSet<>();

  private final Lock lock = new ReentrantLock();
  private final Condition noWorkerOrResource = lock.newCondition();

  // To keep track of workers that are preempted before acknowledged
  private final Set<String> removedBeforeAddedWorkers = new HashSet<>();

  // Terminated
  private boolean terminated = false;

  // Scheduling policy
  private final SchedulingPolicy schedulingPolicy;

  private final AggregateFunctionRepository aggregateFunctionRepository;

  private final Map<String, Set<Integer>> workerAggregateFunctionMap = new HashMap<>();

  /**
   * RunningWorkers constructor.
   */
  @Inject
  RunningWorkers(final SchedulingPolicy schedulingPolicy,
                 final AggregateFunctionRepository aggregateFunctionRepository) {
    this.schedulingPolicy = schedulingPolicy;
    this.aggregateFunctionRepository = aggregateFunctionRepository;
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per vortexWorkerManager.
   */
  void addWorker(final VortexWorkerManager vortexWorkerManager) {
    lock.lock();
    try {
      if (!terminated) {
        if (!removedBeforeAddedWorkers.contains(vortexWorkerManager.getId())) {
          this.runningWorkers.put(vortexWorkerManager.getId(), vortexWorkerManager);
          this.schedulingPolicy.workerAdded(vortexWorkerManager);
          this.workerAggregateFunctionMap.put(vortexWorkerManager.getId(), new HashSet<Integer>());

          // Notify (possibly) waiting scheduler
          noWorkerOrResource.signal();
        }
      } else {
        // Terminate the worker
        vortexWorkerManager.terminate();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per id.
   */
  Optional<Collection<Tasklet>> removeWorker(final String id) {
    lock.lock();
    try {
      if (!terminated) {
        final VortexWorkerManager vortexWorkerManager = this.runningWorkers.remove(id);
        if (vortexWorkerManager != null) {
          this.schedulingPolicy.workerRemoved(vortexWorkerManager);
          return Optional.ofNullable(vortexWorkerManager.removed());
        } else {
          // Called before addWorker (e.g. RM preempted the resource before the Evaluator started)
          removedBeforeAddedWorkers.add(id);
          return Optional.empty();
        }
      } else {
        // No need to return anything since it is terminated
        return Optional.empty();
      }
    } finally {
      try {
        workerAggregateFunctionMap.remove(id);
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Concurrency: Called by single scheduler thread.
   * Parameter: Same tasklet can be launched multiple times.
   */
  void launchTasklet(final Tasklet tasklet) {
    lock.lock();
    try {
      if (!terminated) {
        Optional<String> workerId;
        while(true) {
          workerId = schedulingPolicy.trySchedule(tasklet);
          if (!workerId.isPresent()) {
            try {
              noWorkerOrResource.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          } else {
            break;
          }
        }

        // TODO[JIRA REEF-500]: Will need to support duplicate tasklets.
        if (taskletsToCancel.contains(tasklet.getId())) {
          tasklet.cancelled();
          taskletsToCancel.remove(tasklet.getId());
          LOG.log(Level.FINE, "Cancelled tasklet {0}.", tasklet.getId());
          return;
        }

        final Optional<Integer> taskletAggFunctionId =  tasklet.getAggregateFunctionId();
        final VortexWorkerManager vortexWorkerManager = runningWorkers.get(workerId.get());
        if (taskletAggFunctionId.isPresent() &&
            !workerHasAggregateFunction(vortexWorkerManager.getId(), taskletAggFunctionId.get())) {
          // TODO[JIRA REEF-1130]: fetch aggregate function from repo and send aggregate function to worker.
          throw new NotImplementedException("Serialize aggregate function to worker if it doesn't have it. " +
              "Complete in REEF-1130.");
        }

        vortexWorkerManager.launchTasklet(tasklet);
        schedulingPolicy.taskletLaunched(vortexWorkerManager, tasklet);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same taskletId can come in multiple times.
   */
  void cancelTasklet(final boolean mayInterruptIfRunning, final int taskletId) {
    lock.lock();
    try {
      // This is not ideal since we are using a linear time search on all the workers.
      final String workerId = getWhereTaskletWasScheduledTo(taskletId);
      if (workerId == null) {
        // launchTasklet called but not yet running.
        taskletsToCancel.add(taskletId);
        return;
      }

      if (mayInterruptIfRunning) {
        LOG.log(Level.FINE, "Cancelling running Tasklet with ID {0}.", taskletId);
        runningWorkers.get(workerId).cancelTasklet(taskletId);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same arguments can come in multiple times.
   * (e.g. preemption message coming before tasklet completion message multiple times)
   */
  void doneTasklets(final String workerId, final List<Integer> taskletIds) {
    lock.lock();
    try {
      if (!terminated && runningWorkers.containsKey(workerId)) { // Preemption can come before
        final VortexWorkerManager worker = this.runningWorkers.get(workerId);
        final List<Tasklet> tasklets = worker.taskletsDone(taskletIds);
        this.schedulingPolicy.taskletsDone(worker, tasklets);

        taskletsToCancel.removeAll(taskletIds); // cleanup to prevent memory leak.

        // Notify (possibly) waiting scheduler
        noWorkerOrResource.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  void terminate() {
    lock.lock();
    try {
      if (!terminated) {
        terminated = true;
        for (final VortexWorkerManager vortexWorkerManager : runningWorkers.values()) {
          vortexWorkerManager.terminate();
          schedulingPolicy.workerRemoved(vortexWorkerManager);
        }
        runningWorkers.clear();
      } else {
        throw new RuntimeException("Attempting to terminate an already terminated RunningWorkers");
      }
    } finally {
      lock.unlock();
    }
  }

  boolean isTerminated() {
    return terminated;
  }

  /**
   * Find where a tasklet is scheduled to.
   * @param taskletId id of the tasklet in question
   * @return id of the worker (null if the tasklet was not scheduled to any worker)
   */
  String getWhereTaskletWasScheduledTo(final int taskletId) {
    for (final Map.Entry<String, VortexWorkerManager> entry : runningWorkers.entrySet()) {
      final String workerId = entry.getKey();
      final VortexWorkerManager vortexWorkerManager = entry.getValue();
      if (vortexWorkerManager.containsTasklet(taskletId)) {
        return workerId;
      }
    }
    return null;
  }

  ///////////////////////////////////////// For Tests Only

  /**
   * For unit tests to check whether the worker is running.
   */
  boolean isWorkerRunning(final String workerId) {
    return runningWorkers.containsKey(workerId);
  }

  /**
   * @return true if Vortex has sent the aggregation function to the worker specified by workerId
   */
  private boolean workerHasAggregateFunction(final String workerId, final int aggregateFunctionId) {
    if (!workerAggregateFunctionMap.containsKey(workerId)) {
      LOG.log(Level.WARNING, "Trying to look up a worker's aggregation function for a worker with an ID that has " +
          "not yet been added.");
      return false;
    }

    return workerAggregateFunctionMap.get(workerId).contains(aggregateFunctionId);
  }
}
