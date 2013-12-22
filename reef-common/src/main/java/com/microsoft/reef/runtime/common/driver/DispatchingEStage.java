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

@Private
@DriverSide
public final class DispatchingEStage implements AutoCloseable {

  private class DelayedOnNext {

    public final EventHandler<Object> handler;
    public final Object message;

    public <T, U extends T> DelayedOnNext(final EventHandler<T> handler, final U message) {
      this.handler = (EventHandler<Object>)handler;
      this.message = message;
    }
  }

  private final Map<Class<?>, EventHandler<?>> handlers =
      Collections.synchronizedMap(new MonotonicHashMap<Class<?>, EventHandler<?>>());

  private final REEFErrorHandler errorHandler;
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

  public <T, U extends T> void register(final Class<T> type, final Set<EventHandler<U>> handlers) {
    this.handlers.put(type, new ExceptionHandlingEventHandler<U>(
        new BroadCastEventHandler<U>(handlers), this.errorHandler));
  }

  public <T, U extends T> void onNext(final Class<T> type, final U message) {
    final EventHandler<T> handler = (EventHandler<T>)this.handlers.get(type);
    this.stage.onNext(new DelayedOnNext(handler, message));
  }

  @Override
  public void close() throws Exception {
    this.stage.close();
  }
}
