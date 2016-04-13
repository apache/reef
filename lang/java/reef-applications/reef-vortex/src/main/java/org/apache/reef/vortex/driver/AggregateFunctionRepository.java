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
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.Tuple;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.api.VortexAggregatePolicy;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A repository for {@link VortexAggregateFunction} and its associated {@link VortexAggregatePolicy}.
 */
@ThreadSafe
@Unstable
@Private
final class AggregateFunctionRepository {
  private final ConcurrentMap<Integer, Tuple<VortexAggregateFunction, VortexAggregatePolicy>>
      aggregateFunctionMap = new ConcurrentHashMap<>();

  @Inject
  private AggregateFunctionRepository() {
  }

  /**
   * Associates an aggregate function ID with a {@link VortexAggregateFunction} and a {@link VortexAggregatePolicy}.
   */
  public Tuple<VortexAggregateFunction, VortexAggregatePolicy> put(
      final int aggregateFunctionId,
      final VortexAggregateFunction aggregateFunction,
      final VortexAggregatePolicy policy) {
    return aggregateFunctionMap.put(aggregateFunctionId, new Tuple<>(aggregateFunction, policy));
  }

  /**
   * Gets the {@link VortexAggregateFunction} associated with the aggregate function ID.
   */
  public VortexAggregateFunction getAggregateFunction(final int aggregateFunctionId) {
    return aggregateFunctionMap.get(aggregateFunctionId).getKey();
  }

  /**
   * Gets the {@link VortexAggregatePolicy} associated with the aggregate function ID.
   */
  public VortexAggregatePolicy getPolicy(final int aggregateFunctionId) {
    return aggregateFunctionMap.get(aggregateFunctionId).getValue();
  }
}