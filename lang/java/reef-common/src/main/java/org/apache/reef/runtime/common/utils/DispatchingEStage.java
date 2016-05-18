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
package org.apache.reef.runtime.common.utils;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.util.MonotonicHashMap;
import org.apache.reef.util.ExceptionHandlingEventHandler;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Delayed event router that dispatches messages to the proper event handler by type.
 * This class is used in EvaluatorManager to isolate user threads from REEF.
 */
@Private
@DriverSide
public final class DispatchingEStage implements AutoCloseable {

  /**
   * A map of event handlers, populated in the register() method.
   */
  private final Map<Class<?>, EventHandler<?>> handlers =
      Collections.synchronizedMap(new MonotonicHashMap<Class<?>, EventHandler<?>>());
  /**
   * Exception handler, one for all event handlers.
   */
  private final EventHandler<Throwable> errorHandler;
  /**
   * Thread pool to process delayed event handler invocations.
   */
  private final ThreadPoolStage<DelayedOnNext> stage;

  /**
   * @param errorHandler used for exceptions thrown from the event handlers registered.
   * @param numThreads   number of threads to allocate to dispatch events.
   * @param stageName    the name to use for the underlying stage.
   *                     It will be carried over to name the Thread(s) spawned.
   */
  public DispatchingEStage(final EventHandler<Throwable> errorHandler,
                           final int numThreads,
                           final String stageName) {
    this.errorHandler = errorHandler;
    this.stage = new ThreadPoolStage<>(stageName,
        new EventHandler<DelayedOnNext>() {
          @Override
          public void onNext(final DelayedOnNext promise) {
            promise.handler.onNext(promise.message);
          }
        }, numThreads
    );

  }

  /**
   * Constructs a DispatchingEStage that uses the Thread pool and ErrorHandler of another one.
   *
   * @param other
   */
  public DispatchingEStage(final DispatchingEStage other) {
    this.errorHandler = other.errorHandler;
    this.stage = other.stage;
  }

  /**
   * Register a new event handler.
   *
   * @param type     Message type to process with this handler.
   * @param handlers A set of handlers that process that type of message.
   * @param <T>      Message type.
   * @param <U>      Type of message that event handler supports. Must be a subclass of T.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public <T, U extends T> void register(final Class<T> type, final Set<EventHandler<U>> handlers) {
    this.handlers.put(type, new ExceptionHandlingEventHandler<>(
        new BroadCastEventHandler<>(handlers), this.errorHandler));
  }

  /**
   * Dispatch a new message by type.
   *
   * @param type    Type of event handler - must match the register() call.
   * @param message A message to process. Must be a subclass of T.
   * @param <T>     Message type that event handler supports.
   * @param <U>     input message type. Must be a subclass of T.
   */
  @SuppressWarnings("unchecked")
  public <T, U extends T> void onNext(final Class<T> type, final U message) {
    final EventHandler<T> handler = (EventHandler<T>) this.handlers.get(type);
    this.stage.onNext(new DelayedOnNext(handler, message));
  }

  /**
   * Return true if there are no messages queued or in processing, false otherwise.
   */
  public boolean isEmpty() {
    return this.stage.getQueueLength() + this.stage.getActiveCount() == 0;
  }

  /**
   * Close the internal thread pool.
   *
   * @throws Exception forwarded from EStage.close() call.
   */
  @Override
  public void close() throws Exception {
    this.stage.close();
  }

  /**
   * Delayed EventHandler.onNext() call.
   * Contains a message object and EventHandler to process it.
   */
  private static final class DelayedOnNext {

    private final EventHandler<Object> handler;
    private final Object message;

    @SuppressWarnings("unchecked")
    <T, U extends T> DelayedOnNext(final EventHandler<T> handler, final U message) {
      this.handler = (EventHandler<Object>) handler;
      this.message = message;
    }
  }
}
