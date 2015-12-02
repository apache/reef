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

import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.junit.Assert;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Test correctness of a simple vector calculation on Vortex, checking results with callbacks.
 */
public final class AddOneCallbackTestStart implements VortexStart {
  @Inject
  private AddOneCallbackTestStart() {
  }

  /**
   * Test correctness of a simple vector calculation on Vortex, checking results with callbacks.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final Vector<Integer> inputVector = new Vector<>();
    final int expectedCallbacks = 1000;
    final CountDownLatch latch = new CountDownLatch(expectedCallbacks);
    final ConcurrentSet<Integer> outputSet = new ConcurrentSet<>();
    for (int i = 0; i < expectedCallbacks; i++) {
      inputVector.add(i);
    }

    final List<VortexFuture<Integer>> futures = new ArrayList<>();
    final AddOneFunction addOneFunction = new AddOneFunction();

    for (final int i : inputVector) {
      futures.add(vortexThreadPool.submit(addOneFunction, i, new FutureCallback<Integer>() {
        @Override
        public void onSuccess(final Integer result) {
          outputSet.add(result - 1);
          latch.countDown();
        }

        @Override
        public void onFailure(final Throwable t) {
          throw new RuntimeException("Did not expect exception in test.", t);
        }
      }));
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Assert.assertTrue(outputSet.containsAll(inputVector));
    Assert.assertTrue(inputVector.containsAll(outputSet));
  }
}
