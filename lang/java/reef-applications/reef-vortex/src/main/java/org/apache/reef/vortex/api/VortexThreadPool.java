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
package org.apache.reef.vortex.api;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.driver.VortexMaster;

import javax.inject.Inject;
import java.util.List;

/**
 * Distributed thread pool.
 */
@Unstable
public final class VortexThreadPool {
  private final VortexMaster vortexMaster;

  @Inject
  private VortexThreadPool(final VortexMaster vortexMaster) {
    this.vortexMaster = vortexMaster;
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput, TOutput> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input) {
    return vortexMaster.enqueueTasklet(function, input, Optional.<FutureCallback<TOutput>>empty());
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param callback of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput, TOutput> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input,
             final FutureCallback<TOutput> callback) {
    return vortexMaster.enqueueTasklet(function, input, Optional.of(callback));
  }

  /**
   * @param aggregateFunction to run on VortexFunction outputs
   * @param function to run on Vortex
   * @param inputs of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexAggregationFuture for tracking execution progress of aggregate-able functions
   */
  public <TInput, TOutput> VortexAggregateFuture<TInput, TOutput>
      submit(final VortexAggregateFunction<TOutput> aggregateFunction,
             final VortexFunction<TInput, TOutput> function, final List<TInput> inputs) {
    return vortexMaster.enqueueTasklets(
        aggregateFunction, function, inputs, Optional.<FutureCallback<AggregateResult<TInput, TOutput>>>empty());
  }

  /**
   * @param aggregateFunction to run on VortexFunction outputs
   * @param function to run on Vortex
   * @param inputs of the function
   * @param callback of the aggregation
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexAggregationFuture for tracking execution progress of aggregate-able functions
   */
  public <TInput, TOutput> VortexAggregateFuture<TInput, TOutput>
      submit(final VortexAggregateFunction<TOutput> aggregateFunction,
             final VortexFunction<TInput, TOutput> function, final List<TInput> inputs,
             final FutureCallback<AggregateResult<TInput, TOutput>> callback) {
    return vortexMaster.enqueueTasklets(aggregateFunction, function, inputs, Optional.of(callback));
  }
}