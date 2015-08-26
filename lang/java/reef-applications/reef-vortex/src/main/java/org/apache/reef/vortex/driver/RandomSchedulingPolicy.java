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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * Randomly select a running worker for scheduling a tasklet,
 * without considering worker load or capacity.
 *
 * Concurrency: shares the lock with `RunningWorkers'.
 */
class RandomSchedulingPolicy implements SchedulingPolicy {
  private final Random rand = new Random();

  /**
   * Maintain a list of worker ids using array for random access/selection.
   *
   * Removal from an array might be costly; so instead, we keep removed worker ids in the list,
   * for that the failure/preemption that causes removal is often transient.
   */
  private final List<String> idList = new ArrayList<>();

  /**
   * Set of running worker ids for fast lookup.
   */
  private final HashSet<String> runningSet = new HashSet<>();

  /**
   * Set of preempted (= removed) worker ids for fast lookup.
   */
  private final HashSet<String> preemptedSet = new HashSet<>();

  @Inject
  RandomSchedulingPolicy() {
  }

  /**
   * @param tasklet to schedule
   * @return a random worker
   */
  @Override
  public String trySchedule(final Tasklet tasklet) {
    if (runningSet.size() - preemptedSet.size() == 0) {
      return null;
    }
    int index;
    String workerId;
    do {
      index = rand.nextInt(idList.size());
      workerId = idList.get(index);
    } while (preemptedSet.contains(workerId));
    return workerId;
  }

  /**
   * @param vortexWorker added
   */
  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (!runningSet.contains(workerId)) {
      if (!preemptedSet.remove(workerId)) {
        idList.add(workerId);
        runningSet.add(workerId);
      }
    }
  }

  /**
   * @param vortexWorker removed
   */
  @Override
  public void workerRemoved(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (!runningSet.contains(workerId)) {
      throw new IllegalArgumentException();
    }
    preemptedSet.add(workerId);
  }

  /**
   * Do nothing.
   */
  @Override
  public void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    // Do nothing
  }

  /**
   * Do nothing.
   */
  @Override
  public void taskletCompleted(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    // Do nothing
  }

  /**
   * Do nothing.
   */
  @Override
  public void taskletFailed(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    // Do nothing
  }

}
