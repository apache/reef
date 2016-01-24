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
package org.apache.reef.vortex.common;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.api.VortexFunction;

import java.util.List;

/**
 * A request from the Vortex Driver for the {@link org.apache.reef.vortex.evaluator.VortexWorker} to
 * record aggregate functions for later execution.
 */
@Unstable
@Private
@DriverSide
public final class TaskletAggregationRequest<TInput, TOutput> implements VortexRequest {
  private final int aggregateFunctionId;
  private final VortexAggregateFunction<TOutput> userAggregateFunction;
  private final VortexFunction<TInput, TOutput> function;

  public TaskletAggregationRequest(final int aggregateFunctionId,
                                   final VortexAggregateFunction<TOutput> aggregateFunction,
                                   final VortexFunction<TInput, TOutput> function) {
    this.aggregateFunctionId = aggregateFunctionId;
    this.userAggregateFunction = aggregateFunction;
    this.function = function;
  }

  @Override
  public RequestType getType() {
    return RequestType.AggregateTasklets;
  }

  /**
   * @return the AggregateFunctionID of the aggregate function.
   */
  public int getAggregateFunctionId() {
    return aggregateFunctionId;
  }

  /**
   * @return the aggregate function as specified by the user.
   */
  public VortexAggregateFunction getAggregateFunction() {
    return userAggregateFunction;
  }

  /**
   * @return the user specified function.
   */
  public VortexFunction getFunction() {
    return function;
  }

  /**
   * Execute the aggregate function using the list of outputs.
   * @return Output of the function in a serialized form.
   */
  public byte[] executeAggregation(final List<TOutput> outputs) throws Exception {
    final TOutput output = userAggregateFunction.call(outputs);
    final Codec<TOutput> codec = userAggregateFunction.getOutputCodec();

    // TODO[REEF-1113]: Handle serialization failure separately in Vortex
    return codec.encode(output);
  }

  /**
   * Execute the user specified function.
   */
  public TOutput executeFunction(final TInput input) throws Exception {
    return function.call(input);
  }
}
