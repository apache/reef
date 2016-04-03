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
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.util.Builder;
import org.apache.reef.util.Optional;

/**
 * The policy for local aggregation on the {@link org.apache.reef.vortex.evaluator.VortexWorker}s.
 * The Aggregation function will be triggered on the individual {@link VortexFunction} results on
 * an "OR" basis of what is specified by the policy.
 */
@ClientSide
@Public
@Unstable
public final class VortexAggregatePolicy {
  private Optional<Integer> count;
  private int periodMilliseconds;

  /**
   * No-arg constructor required for Kryo to ser/des.
   */
  VortexAggregatePolicy() {
  }

  private VortexAggregatePolicy(final int periodMilliseconds, final Optional<Integer> count) {
    this.periodMilliseconds = periodMilliseconds;
    this.count = count;
  }

  /**
   * @return the aggregation period in milliseconds.
   */
  public int getPeriodMilliseconds() {
    return periodMilliseconds;
  }

  /**
   * @return the count trigger for the aggregation.
   */
  public Optional<Integer> getCount() {
    return count;
  }

  /**
   * @return a new {@link Builder} for {@link VortexAggregatePolicy}.
   */
  public static AggregatePolicyBuilder newBuilder() {
    return new AggregatePolicyBuilder();
  }

  /**
   * A Builder class for {@link VortexAggregatePolicy}.
   */
  public static final class AggregatePolicyBuilder implements Builder<VortexAggregatePolicy> {
    private Integer periodMilliseconds = null;
    private Optional<Integer> count = Optional.empty();

    private AggregatePolicyBuilder() {
    }

    /**
     * Sets the period to trigger aggregation in milliseconds. Required parameter to build.
     */
    public AggregatePolicyBuilder setTimerPeriodTrigger(final int pOffsetMilliseconds) {
      periodMilliseconds = pOffsetMilliseconds;
      return this;
    }

    /**
     * Sets the count trigger for aggregation. Not required.
     */
    public AggregatePolicyBuilder setCountTrigger(final int pCount) {
      count = Optional.of(pCount);
      return this;
    }

    /**
     * Builds and returns a new {@link VortexAggregatePolicy} based on user's specification.
     * The timer period is a required parameter for this to succeed.
     * @throws IllegalArgumentException if required parameters are not set or if parameters are invalid.
     */
    @Override
    public VortexAggregatePolicy build() throws IllegalArgumentException {
      if (periodMilliseconds == null) {
        throw new IllegalArgumentException("The aggregate period must be set.");
      }

      if (count.isPresent() && count.get() <= 0) {
        throw new IllegalArgumentException("The count trigger must be greater than zero.");
      }

      return new VortexAggregatePolicy(periodMilliseconds, count);
    }
  }
}
