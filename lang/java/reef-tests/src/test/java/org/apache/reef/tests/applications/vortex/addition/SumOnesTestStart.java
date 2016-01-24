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
package org.apache.reef.tests.applications.vortex.addition;

import org.apache.reef.vortex.api.*;
import org.apache.reef.vortex.examples.sumones.AdditionAggregateFunction;
import org.apache.reef.vortex.examples.sumones.IdentityFunction;

import javax.inject.Inject;
import java.util.Vector;

/**
 * Test correctness of an aggregation function that adds integer outputs (ones) on Vortex.
 */
public final class SumOnesTestStart implements VortexStart {
  @Inject
  private SumOnesTestStart() {
  }

  /**
   * Test correctness of an aggregation function that adds integer outputs (ones) on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final int numberOfOnesToSum = 1000;
    final Vector<Integer> inputVector = new Vector<>();
    for (int i = 0; i < numberOfOnesToSum; i++) {
      inputVector.add(1);
    }

    final VortexAggregateFuture<Integer, Integer> future =
        vortexThreadPool.submit(new AdditionAggregateFunction(), new IdentityFunction(), inputVector);

    try {
      AggregateResult<Integer, Integer> result;
      int allSum = 0;

      result = future.get();
      allSum += getAggregateResult(result);

      while (result.hasNext()) {
        result = future.get();

        final int sumResult = getAggregateResult(result);

        int sumInputs = 0;
        for (int i : result.getAggregatedInputs()) {
          sumInputs += 1;
        }

        assert sumResult == sumInputs;
        allSum += sumResult;
      }

      assert allSum == numberOfOnesToSum;

    } catch (final InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private static int getAggregateResult(final AggregateResult<Integer, Integer> result) {
    try {
      return result.getAggregateResult();
    } catch (final VortexAggregateException e) {
      throw new RuntimeException(e);
    }
  }
}
