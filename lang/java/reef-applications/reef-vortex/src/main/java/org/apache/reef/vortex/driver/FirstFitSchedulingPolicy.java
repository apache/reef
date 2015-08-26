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

import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Always select the next worker that has enough resource in a round-robin fashion
 * based on the worker capacity configured.
 *
 * Concurrency: shares the lock with `RunningWorkers'.
 */
class FirstFitSchedulingPolicy implements SchedulingPolicy {

  private final int workerCapacity;

  /**
   * A linked list for a circular buffer of <worker-id, load> pairs.
   */
  private final List<Map.Entry<String, Integer>> loadList = new ArrayList<>();

  /**
   * A list isn't very efficient for fast lookup of worker ids, and so we keep a separate index.
   */
  private final HashMap<String, Integer> indexMap = new HashMap<>();

  /**
   * Invariant: the worker at [lastIndex + 1] in the circular buffer will be selected
   * if the index is valid and the worker has enough resource.
   */
  private int lastIndex = -1;

  @Inject
  FirstFitSchedulingPolicy(@Parameter(VortexMasterConf.WorkerCapacity.class) final int capacity) {
    this.workerCapacity = capacity;
  }

  /**
   * @param tasklet to schedule
   * @return the next worker that has enough resources for the tasklet.
   */
  @Override
  public String trySchedule(final Tasklet tasklet) {
    final int index = loadList.isEmpty() ? 0 : (lastIndex + 1) % loadList.size();
    final String workerId = search(index, loadList.size());
    if (index == 0 || workerId != null) {
      return workerId;
    } else {
      return search(0, index);
    }
  }

  /**
   * @param vortexWorker added
   */
  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (indexMap.containsKey(workerId)) {
      final int index = indexMap.get(workerId);
      loadList.get(index).setValue(0);
    } else {
      loadList.add(new AbstractMap.SimpleEntry<>(workerId, 0));
      indexMap.put(workerId, loadList.size() - 1);
    }
  }

  /**
   * @param vortexWorker removed
   */
  @Override
  public void workerRemoved(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (indexMap.containsKey(workerId)) {
      final Map.Entry<String, Integer> worker = loadList.get(indexMap.get(workerId));
      // We simply mark a removal with worker capacity
      // for that it is often transient
      worker.setValue(workerCapacity);
    }
  }

  /**
   * @param vortexWorker that the tasklet was launched onto
   * @param tasklet launched
   */
  @Override
  public void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    if (indexMap.containsKey(workerId)) {
      final Map.Entry<String, Integer> worker = loadList.get(indexMap.get(workerId));
      worker.setValue(Math.min(workerCapacity, worker.getValue() + 1));
    }
  }

  /**
   * @param vortexWorker that the tasklet completed in
   * @param tasklet completed
   */
  @Override
  public void taskletCompleted(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    removeTasklet(workerId);
  }

  /**
   * @param vortexWorker that the tasklet failed in
   * @param tasklet failed
   */
  @Override
  public void taskletFailed(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    removeTasklet(workerId);
  }

  private String search(final int start, final int end) {
    final ListIterator<Map.Entry<String, Integer>> iter = loadList.listIterator(start);
    int index = start;
    while (iter.hasNext() && index < end) {
      final Map.Entry<String, Integer> worker = iter.next();
      if (worker.getValue() < workerCapacity) {
        lastIndex = index;
        return worker.getKey();
      }
      index++;
    }
    return null;
  }

  private void removeTasklet(final String workerId) {
    if (indexMap.containsKey(workerId)) {
      final Map.Entry<String, Integer> worker = loadList.get(indexMap.get(workerId));
      worker.setValue(Math.max(0, worker.getValue() - 1));
    }
  }

}
