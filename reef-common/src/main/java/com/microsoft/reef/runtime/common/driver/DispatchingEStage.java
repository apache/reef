/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.common.REEFErrorHandler;
import com.microsoft.reef.runtime.common.utils.BroadCastEventHandler;
import com.microsoft.reef.util.ExceptionHandlingEventHandler;
import com.microsoft.tang.util.MonotonicHashMap;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;

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
   * Delayed EventHandler.onNext() call.
   * Contains a message object and EventHandler to process it.
   */
  private class DelayedOnNext {

    public final EventHandler<Object> handler;
    public final Object message;

    public <T, U extends T> DelayedOnNext(final EventHandler<T> handler, final U message) {
      this.handler = (EventHandler<Object>)handler;
      this.message = message;
    }
  }

  /**
   * A map of event handlers, populated in the register() method.
   */
  private final Map<Class<?>, EventHandler<?>> handlers =
      Collections.synchronizedMap(new MonotonicHashMap<Class<?>, EventHandler<?>>());

  /**
   * Exception handler, one for all event handlers.
   */
  private final REEFErrorHandler errorHandler;

  /**
   * Thread pool to process delayed event handler invocations.
   */
  private final EStage<DelayedOnNext> stage;

  public DispatchingEStage(final REEFErrorHandler errorHandler, final int numThreads) {
    this.errorHandler = errorHandler;
    this.stage = new ThreadPoolStage<>(
        new EventHandler<DelayedOnNext>() {
          @Override
          public void onNext(final DelayedOnNext promise) {
            promise.handler.onNext(promise.message);
          }
        }, numThreads);
  }

  /**
   * Register a new event handler.
   * @param type Message type to process with this handler.
   * @param handlers A set of handlers that process that type of message.
   * @param <T> Message type.
   * @param <U> Type of message that event handler supports. Must be a subclass of T.
   */
  public <T, U extends T> void register(final Class<T> type, final Set<EventHandler<U>> handlers) {
    this.handlers.put(type, new ExceptionHandlingEventHandler<>(
        new BroadCastEventHandler<>(handlers), this.errorHandler));
  }

  /**
   * Dispatch a new message by type.
   * @param type Type of event handler - must match the register() call.
   * @param message A message to process. Must be a subclass of T.
   * @param <T> Message type that event handler supports.
   * @param <U> input message type. Must be a subclass of T.
   */
  public <T, U extends T> void onNext(final Class<T> type, final U message) {
    final EventHandler<T> handler = (EventHandler<T>)this.handlers.get(type);
    this.stage.onNext(new DelayedOnNext(handler, message));
  }

  /**
   * Close the internal thread pool.
   * @throws Exception forwarded from EStage.close() call.
   */
  @Override
  public void close() throws Exception {
    this.stage.close();
  }
}
