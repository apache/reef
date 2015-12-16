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

import net.jcip.annotations.NotThreadSafe;
import org.apache.reef.util.Optional;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

/**
 * Randomly select a running worker for scheduling a tasklet,
 * without considering worker load or capacity.
 */
@NotThreadSafe
class RandomSchedulingPolicy implements SchedulingPolicy {
  private final Random rand = new Random();

  /**
   * Keep the worker ids in an array for fast random selection.
   * 
   * Add/removal from the array require O(n) complexity.
   */
  private final List<String> idList = new ArrayList<>();

  @Inject
  RandomSchedulingPolicy() {
  }

  /**
   * @param tasklet to schedule
   * @return a random worker
   */
  @Override
  public Optional<String> trySchedule(final Tasklet tasklet) {
    if (idList.isEmpty()) {
      return Optional.empty();
    } else {
      final int index = rand.nextInt(idList.size());
      return Optional.of(idList.get(index));
    }
  }

  /**
   * @param vortexWorker added
   */
  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    if (idList.indexOf(vortexWorker.getId()) == -1) { // Ignore duplicate add.
      idList.add(vortexWorker.getId());
    }
  }

  /**
   * @param vortexWorker removed
   */
  @Override
  public void workerRemoved(final VortexWorkerManager vortexWorker) {
    idList.remove(vortexWorker.getId()); // Ignore invalid removal.
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
  public void taskletsDone(final VortexWorkerManager vortexWorker, final List<Tasklet> tasklets) {
    // Do nothing
  }
}
