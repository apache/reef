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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.util.Optional;

import java.util.concurrent.*;

/**
 * The interface between user code and submitted task.
 */
@Unstable
public final class VortexFuture<TOutput> implements Future<TOutput> {
  // userResult starts out as null. If not null => variable is set and tasklet returned.
  // Otherwise tasklet has not completed.
  private Optional<TOutput> userResult = null;
  private Exception userException;
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final FutureCallback<TOutput> callbackHandler;

  /**
   * Creates a {@link VortexFuture}.
   */
  public VortexFuture() {
    callbackHandler = null;
  }

  /**
   * Creates a {@link VortexFuture} with a callback.
   */
  public VortexFuture(final FutureCallback<TOutput> callbackHandler) {
    this.callbackHandler = callbackHandler;
  }

  /**
   * TODO[REEF-502]: Support Vortex Tasklet(s) cancellation by user.
   */
  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException("Cancel not yet supported");
  }

  /**
   * TODO[REEF-502]: Support Vortex Tasklet(s) cancellation by user.
   */
  @Override
  public boolean isCancelled() {
    throw new UnsupportedOperationException("Cancel not yet supported");
  }

  /**
   * @return true it the task completed, false if not.
   */
  @Override
  public boolean isDone() {
    return countDownLatch.getCount() == 0;
  }

  /**
   * Infinitely wait for the result of the task.
   */
  @Override
  public TOutput get() throws InterruptedException, ExecutionException {
    countDownLatch.await();
    if (userResult != null) {
      return userResult.get();
    } else {
      assert userException != null;
      throw new ExecutionException(userException);
    }
  }

  /**
   * Wait a certain period of time for the result of the task.
   */
  @Override
  public TOutput get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!countDownLatch.await(timeout, unit)) {
      throw new TimeoutException();
    }

    if (userResult != null) {
      return userResult.get();
    } else {
      assert userException != null;
      throw new ExecutionException(userException);
    }
  }

  /**
   * Called by VortexMaster to let the user know that the task completed.
   */
  public void completed(final TOutput result) {
    this.userResult = Optional.ofNullable(result);
    if (callbackHandler != null) {
      new Thread() {
        @Override
        public void run() {
          callbackHandler.onSuccess(userResult.get());
        }
      }.start();
    }
    this.countDownLatch.countDown();
  }

  /**
   * Called by VortexMaster to let the user know that the task threw an exception.
   */
  public void threwException(final Exception exception) {
    this.userException = exception;
    if (callbackHandler != null) {
      new Thread() {
        @Override
        public void run() {
          callbackHandler.onFailure(exception);
        }
      }.start();
    }
    this.countDownLatch.countDown();
  }
}
