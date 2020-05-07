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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.*;
import org.apache.reef.vortex.protocol.workertomaster.WorkerToMasterReports;

import java.util.List;

/**
 * The heart of Vortex.
 * Processes various tasklet related events/requests coming from different components of the system.
 */
@Unstable
@DriverSide
@DefaultImplementation(DefaultVortexMaster.class)
public interface VortexMaster {
  /**
   * Submit a new Tasklet to be run sometime in the future, with an optional callback function on the result.
   */
  <TInput, TOutput> VortexFuture<TOutput>
      enqueueTasklet(VortexFunction<TInput, TOutput> vortexFunction, TInput input,
                     Optional<FutureCallback<TOutput>> callback);

  /**
   * Submits aggregate-able Tasklets to be run sometime in the future, with an optional callback function on
   * the aggregation progress.
   */
  <TInput, TOutput> VortexAggregateFuture<TInput, TOutput>
      enqueueTasklets(VortexAggregateFunction<TOutput> aggregateFunction,
                      VortexFunction<TInput, TOutput> vortexFunction,
                      VortexAggregatePolicy policy,
                      List<TInput> inputs,
                      Optional<FutureCallback<AggregateResult<TInput, TOutput>>> callback);

  /**
   * Call this when a Tasklet is to be cancelled.
   * @param mayInterruptIfRunning if true, will attempt to cancel running Tasklets; otherwise will only
   *                              prevent a pending Tasklet from running.
   * @param taskletId the ID of the Tasklet.
   */
  void cancelTasklet(boolean mayInterruptIfRunning, int taskletId);

  /**
   * Call this when a new worker is up and running.
   */
  void workerAllocated(VortexWorkerManager vortexWorkerManager);

  /**
   * Call this when a worker is preempted.
   */
  void workerPreempted(String id);

  /**
   * Call this when a worker has reported back.
   */
  void workerReported(String workerId, WorkerToMasterReports workerToMasterReports);

  /**
   * Release all resources and shut down.
   */
  void terminate();
}
