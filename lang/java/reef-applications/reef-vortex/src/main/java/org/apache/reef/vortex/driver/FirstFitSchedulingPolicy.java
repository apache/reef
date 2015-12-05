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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import net.jcip.annotations.NotThreadSafe;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

/**
 * Always select the next worker that has enough resource in a round-robin fashion
 * based on the worker capacity configured.
 */
@NotThreadSafe
class FirstFitSchedulingPolicy implements SchedulingPolicy {

  private final int workerCapacity;

  /**
   * Keep the load information for each worker.
   */
  private final HashMap<String, Integer> idLoadMap = new HashMap<>();

  /**
   * A linked list for a circular buffer of worker ids for search in a round-robin fashion.
   */
  private final List<String> idList = new ArrayList<>();

  /**
   * The index of the next/first worker to check.
   */
  private int nextIndex = 0;
  

  @Inject
  FirstFitSchedulingPolicy(@Parameter(VortexMasterConf.WorkerCapacity.class) final int capacity) {
    this.workerCapacity = capacity;
  }

  /**
   * Checking from nextIndex, choose the first worker that fits to schedule the tasklet onto.
   * @param tasklet to schedule
   * @return the next worker that has enough resources for the tasklet
   */
  @Override
  public Optional<String> trySchedule(final Tasklet tasklet) {
    for (int i = 0; i < idList.size(); i++) {
      final int index = (nextIndex + i) % idList.size();
      final String workerId = idList.get(index);
      
      if (idLoadMap.get(workerId) < workerCapacity) {
        nextIndex = (index + 1) % idList.size();
        return Optional.of(workerId);
      }
    }
    return Optional.empty();
  }

  /**
   * @param vortexWorker added
   */
  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (!idLoadMap.containsKey(workerId)) { // Ignore duplicate add.
      idLoadMap.put(workerId, 0);
      idList.add(nextIndex, workerId); // Prefer to schedule the new worker ASAP.
    }
  }

  /**
   * @param vortexWorker removed
   */
  @Override
  public void workerRemoved(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (idLoadMap.remove(workerId) != null) { // Ignore invalid removal.
      for (int i = 0; i < idList.size(); i++) { // This looping operation might degrade performance.
        if (idList.get(i).equals(workerId)) {
          idList.remove(i);
          
          if (i < nextIndex) {
            nextIndex--;
          } else if (nextIndex == idList.size()) {
            nextIndex = 0;
          }
          return;
        }
      }
    }
  }

  /**
   * @param vortexWorker that the tasklet was launched onto
   * @param tasklet launched
   */
  @Override
  public void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.min(workerCapacity, idLoadMap.get(workerId) + 1));
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

  @Override
  public void taskletCancelled(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    removeTasklet(workerId);
  }

  private void removeTasklet(final String workerId) {
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.max(0, idLoadMap.get(workerId) - 1));
    }
  }
}
