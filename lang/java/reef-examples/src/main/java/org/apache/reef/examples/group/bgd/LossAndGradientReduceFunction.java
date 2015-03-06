/**
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
package org.apache.reef.examples.group.bgd;

import org.apache.reef.examples.group.utils.math.DenseVector;
import org.apache.reef.examples.group.utils.math.Vector;
import org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;

public class LossAndGradientReduceFunction
    implements ReduceFunction<Pair<Pair<Double, Integer>, Vector>> {

  @Inject
  public LossAndGradientReduceFunction() {
  }

  @Override
  public Pair<Pair<Double, Integer>, Vector> apply(
      final Iterable<Pair<Pair<Double, Integer>, Vector>> lags) {

    double lossSum = 0.0;
    int numEx = 0;
    Vector combinedGradient = null;

    for (final Pair<Pair<Double, Integer>, Vector> lag : lags) {
      if (combinedGradient == null) {
        combinedGradient = new DenseVector(lag.second);
      } else {
        combinedGradient.add(lag.second);
      }
      lossSum += lag.first.first;
      numEx += lag.first.second;
    }

    return new Pair<>(new Pair<>(lossSum, numEx), combinedGradient);
  }
}
