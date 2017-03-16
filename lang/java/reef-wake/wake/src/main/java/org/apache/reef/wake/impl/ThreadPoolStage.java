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
package org.apache.reef.wake.impl;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.AbstractEStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.StageConfiguration.*;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.exception.WakeRuntimeException;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage that executes an event handler with a thread pool.
 *
 * @param <T> type
 */
public final class ThreadPoolStage<T> extends AbstractEStage<T> {

  private static final Logger LOG = Logger.getLogger(ThreadPoolStage.class.getName());

  private static final long SHUTDOWN_TIMEOUT = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;

  private final EventHandler<T> handler;
  private final EventHandler<Throwable> errorHandler;
  private final ExecutorService executor;
  private final int numThreads;

  /**
   * Constructs a thread-pool stage.
   *
   * @param handler    the event handler to execute
   * @param numThreads the number of threads to use
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads) {
    this(handler.getClass().getName(), handler, numThreads, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name         the stage name
   * @param handler      the event handler to execute
   * @param numThreads   the number of threads to use
   * @param errorHandler the error handler
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    super(name);
    this.handler = handler;
    this.errorHandler = errorHandler;
    if (numThreads <= 0) {
      throw new WakeRuntimeException(name + " numThreads " + numThreads + " is less than or equal to 0");
    }
    this.numThreads = numThreads;
    this.executor = Executors.newFixedThreadPool(numThreads, new DefaultThreadFactory(name));
    StageManager.instance().register(this);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name       the stage name
   * @param handler    the event handler to execute
   * @param numThreads the number of threads to use
   * @throws WakeRuntimeException
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(NumberOfThreads.class) final int numThreads) {
    this(name, handler, numThreads, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param handler  the event handler to execute
   * @param executor the external executor service provided
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor) {
    this(handler.getClass().getName(), handler, executor);
  }


  /**
   * Constructs a thread-pool stage.
   *
   * @param handler      the event handler to execute
   * @param executor     the external executor service provided
   * @param errorHandler the error handler
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    this(handler.getClass().getName(), handler, executor, errorHandler);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name     the stage name
   * @param handler  the event handler to execute
   * @param executor the external executor service provided
   *                 for consistent tracking, it is recommended to create executor with {@link DefaultThreadFactory}
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor) {
    this(name, handler, executor, null);
  }

  /**
   * Constructs a thread-pool stage.
   *
   * @param name         the stage name
   * @param handler      the event handler to execute
   * @param executor     the external executor service provided
   *                     for consistent tracking, it is recommended to create executor with {@link DefaultThreadFactory}
   * @param errorHandler the error handler
   */
  @Inject
  public ThreadPoolStage(@Parameter(StageName.class) final String name,
                         @Parameter(StageHandler.class) final EventHandler<T> handler,
                         @Parameter(StageExecutorService.class) final ExecutorService executor,
                         @Parameter(ErrorHandler.class) final EventHandler<Throwable> errorHandler) {
    super(name);
    this.handler = handler;
    this.errorHandler = errorHandler;
    this.numThreads = 0;
    this.executor = executor;
    StageManager.instance().register(this);
  }

  /**
   * Handles the event using a thread in the thread pool.
   *
   * @param value the event
   */
  @Override
  @SuppressWarnings("checkstyle:illegalcatch")
  public void onNext(final T value) {
    beforeOnNext();
    try {
      executor.submit(new Runnable() {

        @Override
        public void run() {
          try {
            handler.onNext(value);
          } catch (final Throwable t) {
            if (errorHandler != null) {
              errorHandler.onNext(t);
            } else {
              LOG.log(Level.SEVERE, name + " Exception from event handler", t);
              throw t;
            }
          } finally {
            afterOnNext();
          }
        }

      });
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Encountered error when submitting to executor in ThreadPoolStage.");
      afterOnNext();
      throw e;
    }

  }

  /**
   * Closes resources.
   */
  @Override
  public void close() throws WakeRuntimeException {

    if (closed.compareAndSet(false, true) && numThreads > 0) {

      LOG.log(Level.FINEST, "Closing ThreadPoolStage {0}: begin", this.name);

      executor.shutdown();

      boolean isTerminated = false;
      try {
        isTerminated = executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Interrupted closing ThreadPoolStage " + this.name, ex);
      }

      if (!isTerminated) {
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.SEVERE,
                "Closing ThreadPoolStage {0}: Executor did not terminate in {1} ms. Dropping {2} tasks",
                new Object[]{this.name, SHUTDOWN_TIMEOUT, droppedRunnables.size()});
      }

      if (!executor.isTerminated()) {
        LOG.log(Level.SEVERE, "Closing ThreadPoolStage {0}: Executor failed to terminate.", this.name);

      }

      LOG.log(Level.FINEST, "Closing ThreadPoolStage {0}: end", this.name);

    }
  }

  /**
   * Returns true if resources are closed.
   */
  public boolean isClosed() {
    return closed.get() && executor.isTerminated();
  }

  /**
   * Gets the queue length of this stage.
   *
   * @return the queue length
   */
  public int getQueueLength() {
    return ((ThreadPoolExecutor) executor).getQueue().size();
  }

  /**
   * Gets the active count of this stage.
   * @return the active count
   */
  public int getActiveCount() {
    return (int)(getInMeter().getCount() - getOutMeter().getCount());
  }
}
