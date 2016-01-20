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
package org.apache.reef.tests.applications.vortex.addone;

import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * Test correctness of a simple vector calculation on Vortex.
 */
public final class AddOneTestStart implements VortexStart {
  @Inject
  private AddOneTestStart() {
  }

  /**
   * Test correctness of a simple vector calculation on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final Vector<Integer> inputVector = new Vector<>();
    for (int i = 0; i < 1000; i++) {
      inputVector.add(i);
    }

    final List<VortexFuture<Integer>> futures = new ArrayList<>();
    final AddOneFunction addOneFunction = new AddOneFunction();
    for (final int i : inputVector) {
      futures.add(vortexThreadPool.submit(addOneFunction, i));
    }

    int j = 0;
    for (final VortexFuture<Integer> future : futures) {
      try {
        if (j+1 != future.get()) {
          throw new RuntimeException(); // throw an exception if the result is not what we expected
        }
        j++;
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}