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
package org.apache.reef.tests.applications.vortex.exception;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.junit.Assert;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Test that an {@link ExecutionException} is thrown on tasklets that throws an Exception, and
 * that the callback handler gets invoked on failure.
 */
public final class ExceptionCallbackTestStart implements VortexStart {

  @Inject
  private ExceptionCallbackTestStart() {
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final CountDownLatch latch = new CountDownLatch(1);

    final ExceptionFunction exceptionFunction = new ExceptionFunction();

    final VortexFuture<Integer> future = vortexThreadPool.submit(exceptionFunction, 1, new FutureCallback<Integer>() {
      @Override
      public void onSuccess(final Integer result) {
        throw new RuntimeException("Did not expect success in test.");
      }

      @Override
      public void onFailure(final Throwable t) {
        latch.countDown();
      }
    });

    boolean gotFailure = false;
    try {
      latch.await();
      future.get();
    } catch (final InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    } catch (final ExecutionException ex) {
      gotFailure = true;
    }

    Assert.assertTrue(gotFailure);
  }
}
