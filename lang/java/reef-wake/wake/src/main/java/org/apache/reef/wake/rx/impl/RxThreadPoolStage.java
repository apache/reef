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
package org.apache.reef.wake.rx.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.StageConfiguration.NumberOfThreads;
import org.apache.reef.wake.StageConfiguration.StageName;
import org.apache.reef.wake.StageConfiguration.StageObserver;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.exception.WakeRuntimeException;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.impl.StageManager;
import org.apache.reef.wake.rx.AbstractRxStage;
import org.apache.reef.wake.rx.Observer;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage that executes the observer with a thread pool.
 * <p>
 * {@code onNext}'s will be arbitrarily subject to reordering, as with most stages.
 * <p>
 * All {@code onNext}'s for which returning from the method call
 * happens-before the call to {@code onComplete} will maintain
 * this relationship when passed to the observer.
 * <p>
 * Any {@code onNext} whose return is not ordered before
 * {@code onComplete} may or may not get dropped.
 *
 * @param <T> type of event
 */
public final class RxThreadPoolStage<T> extends AbstractRxStage<T> {
  private static final Logger LOG = Logger.getLogger(RxThreadPoolStage.class.getName());

  private final Observer<T> observer;
  private final ExecutorService executor;
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;
  private ExecutorService completionExecutor;
  private DefaultThreadFactory tf;

  /**
   * Constructs a Rx thread pool stage.
   *
   * @param observer   the observer to execute
   * @param numThreads the number of threads
   */
  @Inject
  public RxThreadPoolStage(@Parameter(StageObserver.class) final Observer<T> observer,
                           @Parameter(NumberOfThreads.class) final int numThreads) {
    this(observer.getClass().getName(), observer, numThreads);
  }

  /**
   * Constructs a Rx thread pool stage.
   *
   * @param name       the stage name
   * @param observer   the observer to execute
   * @param numThreads the number of threads
   */
  @Inject
  public RxThreadPoolStage(@Parameter(StageName.class) final String name,
                           @Parameter(StageObserver.class) final Observer<T> observer,
                           @Parameter(NumberOfThreads.class) final int numThreads) {
    super(name);
    this.observer = observer;
    if (numThreads <= 0) {
      throw new WakeRuntimeException(name + " numThreads " + numThreads + " is less than or equal to 0");
    }
    tf = new DefaultThreadFactory(name);
    this.executor = Executors.newFixedThreadPool(numThreads, tf);
    this.completionExecutor = Executors.newSingleThreadExecutor(tf);
    StageManager.instance().register(this);
  }

  /**
   * Provides the observer with the new value.
   *
   * @param value the new value
   */
  @Override
  public void onNext(final T value) {
    beforeOnNext();
    executor.submit(new Runnable() {

      @Override
      public void run() {
        observer.onNext(value);
        afterOnNext();
      }
    });
  }

  /**
   * Notifies the observer that the provider has experienced an error
   * condition.
   *
   * @param error the error
   */
  @Override
  public void onError(final Exception error) {
    submitCompletion(new Runnable() {

      @Override
      public void run() {
        observer.onError(error);
      }

    });
  }

  /**
   * Notifies the observer that the provider has finished sending push-based
   * notifications.
   */
  @Override
  public void onCompleted() {
    submitCompletion(new Runnable() {

      @Override
      public void run() {
        observer.onCompleted();
      }

    });
  }

  private void submitCompletion(final Runnable r) {
    executor.shutdown();
    completionExecutor.submit(new Runnable() {

      @Override
      public void run() {
        try {
          // no timeout for completion, only close()
          if (!executor.awaitTermination(3153600000L, TimeUnit.SECONDS)) {
            TimeoutException e = new TimeoutException("Executor terminated due to unrequired timeout");
            LOG.log(Level.SEVERE, e.getMessage());
            observer.onError(e);
          }
        } catch (final InterruptedException e) {
          e.printStackTrace();
          observer.onError(e);
        }
        r.run();
      }
    });
  }

  /**
   * Closes the stage.
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executor.shutdown();
      completionExecutor.shutdown();
      if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
      if (!completionExecutor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = completionExecutor.shutdownNow();
        LOG.log(Level.WARNING, "Completion executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

  /**
   * Gets the queue length of this stage.
   *
   * @return the queue length
   */
  public int getQueueLength() {
    return ((ThreadPoolExecutor) executor).getQueue().size();
  }
}
