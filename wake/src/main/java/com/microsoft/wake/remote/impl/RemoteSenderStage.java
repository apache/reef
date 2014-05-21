/**
 * Copyright (C) 2014 Microsoft Corporation
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

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Stage;
import com.microsoft.wake.WakeParameters;
import com.microsoft.wake.impl.DefaultThreadFactory;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;
import com.microsoft.wake.remote.transport.Transport;

/**
 * Stage to manage resources related to sending event remotely
 * 
 * @param <T> type
 */
public class RemoteSenderStage implements Stage {

  private static final Logger LOG = Logger.getLogger(RemoteSenderStage.class.getName());
  
  private final ExecutorService executor;
  private final Encoder encoder;
  private final Transport transport;
  private final long shutdownTimeout = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;
  
  /**
   * Constructs a remote sender stage
   * 
   * @param encoder the encoder of the event
   * @param transport the transport to send events
   */
  public RemoteSenderStage(Encoder encoder, Transport transport) {
    this.executor = Executors.newCachedThreadPool(new DefaultThreadFactory(RemoteSenderStage.class.getName()));
    this.encoder = encoder;
    this.transport = transport;
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
   * 
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close {0}", transport);
    executor.shutdown();
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