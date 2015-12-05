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

package org.apache.reef.tests.applications.vortex.cancellation;

import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.junit.Assert;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests the cancellation of a tasklet.
 */
public final class TaskletCancellationTestStart implements VortexStart {

  @Inject
  private TaskletCancellationTestStart() {
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final InfiniteLoopWithCancellationFunction function = new InfiniteLoopWithCancellationFunction();
    final VortexFuture future = vortexThreadPool.submit(function, 0);

    try {
      future.get(10, TimeUnit.SECONDS); // Hacky way to increase probability that the task has been launched.
    } catch (final TimeoutException e) {
      // Harmless.
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Unexpected exception.");
    }

    Assert.assertTrue(future.cancel(true));

    try {
      future.get();
      Assert.fail();
    } catch (final ExecutionException e) {
      // Expected.
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
  }
}
