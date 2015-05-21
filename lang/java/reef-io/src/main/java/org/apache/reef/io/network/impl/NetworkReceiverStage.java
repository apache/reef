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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.TransportEvent;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receive TransportEvents from Transport and transfer them to handlers in ThreadPool
 */
final class NetworkReceiverStage implements EStage<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(NetworkReceiverStage.class.getName());
  private final long shutdownTimeout = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private final EventHandler<TransportEvent> handler;
  private final ThreadPoolStage<TransportEvent> stage;
  private final ExecutorService executor;

  NetworkReceiverStage(
      final EventHandler<TransportEvent> handler,
      final EventHandler<Throwable> errorHandler,
      final int numThreads) {

    this.handler = handler;
    this.executor = Executors.newFixedThreadPool(
        numThreads, new DefaultThreadFactory(NetworkReceiverStage.class.getName()));
    this.stage = new ThreadPoolStage<>(this.handler, this.executor, errorHandler);
  }

  /**
   * Handles the received event
   *
   * @param value the event
   */
  @Override
  public void onNext(TransportEvent value) {
    LOG.log(Level.FINEST, "{0}", value);
    stage.onNext(value);
  }

  /**
   * Closes the stage
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close");

    if (this.executor != null) {
      this.executor.shutdown();
      try {
        // wait for threads to finish for timeout
        if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in {0} ms.", shutdownTimeout);
          List<Runnable> droppedRunnables = executor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped {0} tasks.", droppedRunnables.size());
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Close interrupted", e);
        throw new NetworkRuntimeException(e);
      }
    }
  }
}
