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
import org.apache.reef.runtime.common.utils.BroadCastEventHandler;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * The interface between user code and submitted task.
 * TODO[REEF-505]: Callback features for VortexFuture.
 */
@Unstable
public final class VortexFuture<TOutput> implements Future<TOutput> {
  private TOutput userResult;
  private Exception userException;
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final ThreadPoolStage<TOutput> stage;

  public VortexFuture(final Collection<EventHandler<TOutput>> callbackHandlers) {
    stage = new ThreadPoolStage<>(new BroadCastEventHandler(callbackHandlers), 1);
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
      return userResult;
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
      return userResult;
    } else {
      assert userException != null;
      throw new ExecutionException(userException);
    }
  }

  /**
   * Called by VortexMaster to let the user know that the task completed.
   */
  public void completed(final TOutput result) {
    this.userResult = result;
    stage.onNext(userResult);
    this.countDownLatch.countDown();
  }

  /**
   * Called by VortexMaster to let the user know that the task threw an exception.
   */
  public void threwException(final Exception exception) {
    this.userException = exception;
    this.countDownLatch.countDown();
  }
}
