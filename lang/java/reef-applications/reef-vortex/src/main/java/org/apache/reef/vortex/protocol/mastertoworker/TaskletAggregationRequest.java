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
package org.apache.reef.vortex.protocol.mastertoworker;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.api.VortexAggregatePolicy;
import org.apache.reef.vortex.api.VortexFunction;

import java.util.List;

/**
 * A request from the Vortex Driver for the {@link org.apache.reef.vortex.evaluator.VortexWorker} to
 * record aggregate functions for later execution.
 */
@Unstable
@Private
@DriverSide
public final class TaskletAggregationRequest<TInput, TOutput> implements MasterToWorkerRequest {
  private int aggregateFunctionId;
  private VortexAggregateFunction<TOutput> userAggregateFunction;
  private VortexFunction<TInput, TOutput> function;
  private VortexAggregatePolicy policy;

  /**
   * No-arg constructor required for Kryo to serialize/deserialize.
   */
  TaskletAggregationRequest() {
  }

  public TaskletAggregationRequest(final int aggregateFunctionId,
                                   final VortexAggregateFunction<TOutput> aggregateFunction,
                                   final VortexFunction<TInput, TOutput> function,
                                   final VortexAggregatePolicy policy) {
    this.aggregateFunctionId = aggregateFunctionId;
    this.userAggregateFunction = aggregateFunction;
    this.function = function;
    this.policy = policy;
  }

  @Override
  public Type getType() {
    return Type.AggregateTasklets;
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
   * @return the aggregation policy.
   */
  public VortexAggregatePolicy getPolicy() {
    return policy;
  }

  /**
   * Execute the aggregate function using the list of outputs.
   * @return Output of the function.
   */
  public TOutput executeAggregation(final List<TOutput> outputs) throws Exception {
    return userAggregateFunction.call(outputs);
  }

  /**
   * Execute the user specified function.
   */
  public TOutput executeFunction(final TInput input) throws Exception {
    return function.call(input);
  }
}
