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

import com.google.common.util.concurrent.FutureCallback;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;

import java.io.Serializable;

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
  <TInput extends Serializable, TOutput extends Serializable> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> vortexFunction, final TInput input,
                     final Optional<FutureCallback<TOutput>> callback);

  /**
   * Call this when a new worker is up and running.
   */
  void workerAllocated(final VortexWorkerManager vortexWorkerManager);

  /**
   * Call this when a worker is preempted.
   */
  void workerPreempted(final String id);

  /**
   * Call this when a Tasklet is completed.
   */
  void taskletCompleted(final String workerId, final int taskletId, final Serializable result);

  /**
   * Call this when a Tasklet errored.
   */
  void taskletErrored(final String workerId, final int taskletId, final Exception exception);

  /**
   * Release all resources and shut down.
   */
  void terminate();
}
