/**
 * Copyright (C) 2012 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.remote.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.impl.DefaultThreadFactory;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.impl.WakeUncaughtExceptionHandler;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;

/**
 * Receive incoming events and dispatch to correct handlers
 */
public class RemoteReceiverStage implements EStage<TransportEvent> {
  
  private static final Logger LOG = Logger.getLogger(RemoteReceiverStage.class.getName());
  
  private final EventHandler<TransportEvent> handler;
  private final ThreadPoolStage<TransportEvent> stage;
  private final ExecutorService executor; // for decoupling
  
  private final long shutdownTimeout = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  /**
   * Constructs a remote receiver stage
   * 
   * @param handler the handler of remote events
   * @param errorHandler the exception handler
   */
  public RemoteReceiverStage(EventHandler<RemoteEvent<byte[]>> handler, EventHandler<Throwable> errorHandler) {
    this.handler = new RemoteReceiverEventHandler(handler);
    this.executor = Executors.newCachedThreadPool(new DefaultThreadFactory(RemoteReceiverStage.class.getName()));
    this.stage = new ThreadPoolStage<TransportEvent>(this.handler, this.executor, errorHandler);
  }
  
  @Deprecated
  public RemoteReceiverStage(EventHandler<RemoteEvent<byte[]>> handler) {
    this(handler, null);
  }
  
  @Deprecated
  public RemoteReceiverStage(EventHandler<RemoteEvent<byte[]>> handler, boolean flag) {
    this(handler, null);
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
   * 
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close");

    if (this.executor != null) {
      this.executor.shutdown();
      try {
        // wait for threads to finish for timeout
        if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
          List<Runnable> droppedRunnables = executor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Close interrupted");
        throw new RemoteRuntimeException(e);
      }
    }
  }

}
