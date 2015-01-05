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
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;

public class LineSearchReduceFunction implements Reduce.ReduceFunction<Pair<Vector, Integer>> {

  @Inject
  public LineSearchReduceFunction() {
  }

  @Override
  public Pair<Vector, Integer> apply(final Iterable<Pair<Vector, Integer>> evals) {

    Vector combinedEvaluations = null;
    int numEx = 0;

    for (final Pair<Vector, Integer> eval : evals) {
      if (combinedEvaluations == null) {
        combinedEvaluations = new DenseVector(eval.first);
      } else {
        combinedEvaluations.add(eval.first);
      }
      numEx += eval.second;
    }

    return new Pair<>(combinedEvaluations, numEx);
  }
}
