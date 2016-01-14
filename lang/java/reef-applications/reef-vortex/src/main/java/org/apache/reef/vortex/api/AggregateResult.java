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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.util.Optional;

import java.util.Collections;
import java.util.List;

/**
 * The result of an aggregate.
 */
@Public
@ClientSide
@Unstable
public final class AggregateResult<TInput, TOutput> {

  private final Optional<TOutput> aggregatedOutput;
  private final List<Pair<TInput, VortexFunction<TInput, TOutput>>> aggregatedList;
  private final boolean hasNext;
  private final Optional<Exception> exception;

  @Private
  public AggregateResult(final Exception exception,
                         final List<Pair<TInput, VortexFunction<TInput, TOutput>>> aggregatedList,
                         final boolean hasNext) {
    this(Optional.<TOutput>empty(), Optional.of(exception), aggregatedList, hasNext);
  }

  @Private
  public AggregateResult(final TOutput aggregatedOutput,
                         final List<Pair<TInput, VortexFunction<TInput, TOutput>>> aggregatedList,
                         final boolean hasNext) {
    this(Optional.of(aggregatedOutput), Optional.<Exception>empty(), aggregatedList, hasNext);
  }

  private AggregateResult(final Optional<TOutput> aggregatedOutput,
                          final Optional<Exception> exception,
                          final List<Pair<TInput, VortexFunction<TInput, TOutput>>> aggregatedList,
                          final boolean hasNext) {
    this.aggregatedOutput = aggregatedOutput;
    this.aggregatedList = Collections.unmodifiableList(aggregatedList);
    this.hasNext = hasNext;
    this.exception = exception;
  }

  /**
   * @return the output of an aggregation, throws the Exception if a Tasklet or an aggregation fails.
   * If an aggregation fails, {@link VortexAggregateException} will be thrown, otherwise
   * the Exception that caused the Tasklet to fail will be thrown directly.
   * @throws Exception the Exception that caused the Tasklet or aggregation failure.
   */
  public TOutput getAggregateResult() throws Exception {
    if (exception.isPresent()) {
      throw exception.get();
    }

    return aggregatedOutput.get();
  }

  /**
   * @return the associated input and VortexFunction of an aggregation.
   */
  public List<Pair<TInput, VortexFunction<TInput, TOutput>>> getAggregatedInputAndFunction() {
    return aggregatedList;
  }

  /**
   * If an aggregation fails, {@link VortexAggregateException} will be thrown, otherwise
   * the Exception that caused the Tasklet to fail will be thrown directly.
   * @return the Exception that caused the Tasklet or aggregation failure, if any.
   */
  public Optional<Exception> getException() {
    return exception;
  }

  /**
   * @return true if more results will be available, false otherwise.
   */
  public boolean hasNext() {
    return hasNext;
  }
}
