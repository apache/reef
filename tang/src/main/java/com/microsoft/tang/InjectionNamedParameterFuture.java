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
package com.microsoft.tang;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * There is no reason to use this instead of InjectionFuture.
 */
@Deprecated
public class InjectionNamedParameterFuture<T> implements Future<T> {

  private final Injector injector;

  private final Class<? extends Name<T>> parameter;

  private T cached = null;

  public InjectionNamedParameterFuture(final Injector injector,
      Class<? extends Name<T>> parameter) {
    this.injector = injector;
    this.parameter = parameter;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return false;
  }

  @Override
  public T get() {
    try {
      if (this.cached == null) {
        this.cached = this.injector.getNamedInstance(this.parameter);
      }
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
    return this.cached;
  }

  @Override
  public T get(long timeout, TimeUnit unit) {
    return get();
  }

}
