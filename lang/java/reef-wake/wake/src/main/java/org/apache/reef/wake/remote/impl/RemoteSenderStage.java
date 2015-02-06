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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.transport.Transport;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage to manage resources related to sending event remotely.
 */
public class RemoteSenderStage implements Stage {

  private static final long SHUTDOWN_TIMEOUT = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private static final Logger LOG = Logger.getLogger(RemoteSenderStage.class.getName());

  private final ExecutorService executor;
  private final Encoder encoder;
  private final Transport transport;

  /**
   * Constructs a remote sender stage
   *
   * @param encoder    the encoder of the event
   * @param transport  the transport to send events
   * @param numThreads the number of threads
   */
  public RemoteSenderStage(final Encoder encoder, final Transport transport, final int numThreads) {
    this.encoder = encoder;
    this.transport = transport;
    this.executor = Executors.newFixedThreadPool(
        numThreads, new DefaultThreadFactory(RemoteSenderStage.class.getName()));
  }

  /**
   * Returns a new remote sender event handler
   *
   * @return a remote sender event handler
   */
  public <T> EventHandler<RemoteEvent<T>> getHandler() {
    return new RemoteSenderEventHandler<T>(encoder, transport, executor);
  }

  /**
   * Closes the stage
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close {0}", transport);
    executor.shutdown();
    try {
      // wait for threads to finish for timeout
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in {0} ms.", SHUTDOWN_TIMEOUT);
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped {0} tasks.", droppedRunnables.size());
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Close interrupted", e);
      throw new RemoteRuntimeException(e);
    }
  }
}
