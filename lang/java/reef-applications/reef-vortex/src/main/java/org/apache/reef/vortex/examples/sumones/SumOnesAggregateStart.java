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
package org.apache.reef.vortex.examples.sumones;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.*;

import javax.inject.Inject;
import java.util.Vector;

/**
 * SumOnes User Code Example.
 */
final class SumOnesAggregateStart implements VortexStart {

  private final int numbers;

  @Inject
  private SumOnesAggregateStart(@Parameter(SumOnes.NumberOfOnes.class) final int numbers) {
    this.numbers = numbers;
  }

  /**
   * Perform a simple sum and aggregation of ones on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final Vector<Integer> inputVector = new Vector<>();
    for (int i = 0; i < numbers; i++) {
      inputVector.add(1);
    }

    final VortexAggregateFuture<Integer, Integer> future =
        vortexThreadPool.submit(new AdditionAggregateFunction(), new IdentityFunction(), inputVector);

    try {
      AggregateResult<Integer, Integer> result;
      result = future.get();
      int allSum = 0;
      while (result.hasNext()) {
        result = future.get();
        final int sumResult;

        try {
          sumResult = result.getAggregateResult();
        } catch (final VortexAggregateException e) {
          throw new RuntimeException(e);
        }

        int sumInputs = 0;
        for (int i : result.getAggregatedInputs()) {
          sumInputs += i;
        }

        assert sumResult == sumInputs;

        allSum += sumResult;
      }

      assert allSum == numbers;

    } catch (final InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
