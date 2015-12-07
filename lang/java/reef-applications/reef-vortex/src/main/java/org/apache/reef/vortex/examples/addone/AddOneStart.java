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
package org.apache.reef.vortex.examples.addone;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.api.VortexStart;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * AddOne User Code Example.
 */
final class AddOneStart implements VortexStart {
  private final int dimension;

  @Inject
  private AddOneStart(@Parameter(AddOne.Dimension.class) final int dimension) {
    this.dimension = dimension;
  }

  /**
   * Perform a simple vector calculation on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final Vector<Integer> inputVector = new Vector<>();
    for (int i = 0; i < dimension; i++) {
      inputVector.add(i);
    }

    final List<VortexFuture<Integer>> futures = new ArrayList<>();
    final AddOneFunction addOneFunction = new AddOneFunction();
    for (final int i : inputVector) {
      futures.add(vortexThreadPool.submit(addOneFunction, i));
    }

    final Vector<Integer> outputVector = new Vector<>();
    for (final VortexFuture<Integer> future : futures) {
      try {
        outputVector.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    System.out.println("RESULT:");
    System.out.println(outputVector);
  }
}
