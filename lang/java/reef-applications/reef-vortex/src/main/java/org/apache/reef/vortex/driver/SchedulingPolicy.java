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

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

import java.util.List;

/**
 * For choosing which worker to schedule the tasklet onto.
 */
@DefaultImplementation(FirstFitSchedulingPolicy.class)
interface SchedulingPolicy {
  /**
   * Implementation of this method is expected to be fast.
   * @param tasklet to schedule
   * @return the worker onto which the tasklet should be scheduled, null if there's none
   */
  Optional<String> trySchedule(final Tasklet tasklet);

  /**
   * Worker added.
   */
  void workerAdded(final VortexWorkerManager vortexWorker);

  /**
   * Worker removed.
   */
  void workerRemoved(final VortexWorkerManager vortexWorker);

  /**
   * Tasklet launched.
   */
  void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet);

  /**
   * Tasklets completed.
   */
  void taskletsDone(final VortexWorkerManager vortexWorker, final List<Tasklet> tasklets);
}
