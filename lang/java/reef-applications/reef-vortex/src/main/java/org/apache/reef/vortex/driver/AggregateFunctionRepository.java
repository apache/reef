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
import org.apache.reef.vortex.api.VortexAggregateFunction;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A repository for {@link VortexAggregateFunction}, used to pass functions between {@link VortexMaster} and
 * {@link RunningWorkers}.
 */
@ThreadSafe
@Unstable
@Private
public final class AggregateFunctionRepository {
  private final Map<Integer, VortexAggregateFunction> aggregateFunctionIdMap = new ConcurrentHashMap<>();

  @Inject
  private AggregateFunctionRepository() {
  }

  VortexAggregateFunction put(final int aggregateFunctionId, final VortexAggregateFunction function) {
    return aggregateFunctionIdMap.put(aggregateFunctionId, function);
  }

  VortexAggregateFunction get(final int aggregateFunctionId) {
    return aggregateFunctionIdMap.get(aggregateFunctionId);
  }
}