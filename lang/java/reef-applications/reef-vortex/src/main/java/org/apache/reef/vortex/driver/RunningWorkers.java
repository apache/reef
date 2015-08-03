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

import javax.inject.Inject;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keeps track of all running VortexWorkers and Tasklets.
 * Upon Tasklet launch request, randomly schedules it to a VortexWorkerManager.
 */
@ThreadSafe
@DriverSide
final class RunningWorkers {
  // RunningWorkers and its locks
  private final HashMap<String, VortexWorkerManager> runningWorkers = new HashMap<>(); // Running workers/tasklets
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock readLock = rwLock.readLock(); // Need to acquire this to launch/complete tasklets
  private final Lock writeLock = rwLock.writeLock(); // Need to acquire this to add/remove worker
  private final Condition noRunningWorker = writeLock.newCondition(); // When there's no running worker

  // To keep track of workers that are preempted before acknowledged
  private final Set<String> removedBeforeAddedWorkers = new HashSet<>();

  // Terminated
  private volatile boolean terminated = false;

  /**
   * RunningWorkers constructor.
   */
  @Inject
  RunningWorkers() {
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per vortexWorkerManager.
   */
  void addWorker(final VortexWorkerManager vortexWorkerManager) {
    if (!terminated) {
      writeLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          if (!removedBeforeAddedWorkers.contains(vortexWorkerManager.getId())) {
            this.runningWorkers.put(vortexWorkerManager.getId(), vortexWorkerManager);

            // Notify (possibly) waiting scheduler
            if (runningWorkers.size() == 1) {
              noRunningWorker.signalAll();
            }
          }
          return;
        }
      } finally {
        writeLock.unlock();
      }
    }

    // Terminate the worker
    vortexWorkerManager.terminate();
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per id.
   */
  Collection<Tasklet> removeWorker(final String id) {
    if (!terminated) {
      writeLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          final VortexWorkerManager vortexWorkerManager = this.runningWorkers.remove(id);
          if (vortexWorkerManager != null) {
            return vortexWorkerManager.removed();
          } else {
            // Called before addWorker (e.g. RM preempted the resource before the Evaluator started)
            removedBeforeAddedWorkers.add(id);
            return new ArrayList<>(0);
          }
        }
      } finally {
        writeLock.unlock();
      }
    }

    // No need to return anything since it is terminated
    return new ArrayList<>(0);
  }

  /**
   * Concurrency: Called by single scheduler thread.
   * Parameter: Same tasklet can be launched multiple times.
   */
  void launchTasklet(final Tasklet tasklet) {
    if (!terminated) {
      readLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          // Wait until there is a running worker
          if (runningWorkers.isEmpty()) {
            readLock.unlock();
            writeLock.lock();
            try {
              while (runningWorkers.isEmpty()) {
                try {
                  noRunningWorker.await();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
              readLock.lock();
            } finally {
              writeLock.unlock();
            }
          }

          final VortexWorkerManager vortexWorkerManager = randomlyChooseWorker();
          vortexWorkerManager.launchTasklet(tasklet);
          return;
        }
      } finally {
        readLock.unlock();
      }
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same arguments can come in multiple times.
   * (e.g. preemption message coming before tasklet completion message multiple times)
   */
  void completeTasklet(final String workerId,
                              final int taskletId,
                              final Serializable result) {
    if (!terminated) {
      readLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          if (runningWorkers.containsKey(workerId)) { // Preemption can come before
            this.runningWorkers.get(workerId).taskletCompleted(taskletId, result);
          }
          return;
        }
      } finally {
        readLock.unlock();
      }
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same arguments can come in multiple times.
   * (e.g. preemption message coming before tasklet error message multiple times)
   */
  void errorTasklet(final String workerId,
                           final int taskletId,
                           final Exception exception) {
    if (!terminated) {
      readLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          if (runningWorkers.containsKey(workerId)) { // Preemption can come before
            this.runningWorkers.get(workerId).taskletThrewException(taskletId, exception);
          }
          return;
        }
      } finally {
        readLock.unlock();
      }
    }
  }

  void terminate() {
    if (!terminated) {
      writeLock.lock();
      try {
        if (!terminated) { // Make sure it is really terminated
          terminated = true;
          for (final VortexWorkerManager vortexWorkerManager : runningWorkers.values()) {
            vortexWorkerManager.terminate();
          }
          runningWorkers.clear();
          return;
        }
      } finally {
        writeLock.unlock();
      }
    }
    throw new RuntimeException("Attempting to terminate an already terminated RunningWorkers");
  }

  boolean isTerminated() {
    return terminated;
  }

  private VortexWorkerManager randomlyChooseWorker() {
    final Collection<VortexWorkerManager> workers = runningWorkers.values();
    final int index = new Random().nextInt(workers.size());
    int i = 0;
    for (final VortexWorkerManager vortexWorkerManager : workers) {
      if (i == index) {
        return vortexWorkerManager;
      } else {
        i++;
      }
    }
    throw new RuntimeException("Bad Index");
  }

  ///////////////////////////////////////// For Tests Only

  /**
   * For unit tests to check whether the tasklet has been scheduled and running.
   */
  boolean isTaskletRunning(final int taskletId) {
    for (final VortexWorkerManager vortexWorkerManager : runningWorkers.values()) {
      if (vortexWorkerManager.containsTasklet(taskletId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * For unit tests to check whether the worker is running.
   */
  boolean isWorkerRunning(final String workerId) {
    return runningWorkers.containsKey(workerId);
  }

  /**
   * For unit tests to see where a tasklet is scheduled to.
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
}
