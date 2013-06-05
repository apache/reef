package com.microsoft.tang;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;

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
