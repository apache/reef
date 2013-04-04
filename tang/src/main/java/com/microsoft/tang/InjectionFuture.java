package com.microsoft.tang;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.microsoft.tang.exceptions.InjectionException;

public class InjectionFuture<T> implements Future<T> {

  protected final Injector injector;

  private final Class<? extends T> iface;

  private T cached = null;

  public InjectionFuture(final Injector injector, Class<? extends T> iface) {
    this.injector = injector;
    this.iface = iface;
  }

  @Override
  public final boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public final boolean isCancelled() {
    return false;
  }

  @Override
  public final boolean isDone() {
    return this.cached != null;
  }

  @Override
  public T get() {
    if (this.cached == null) {
      synchronized (this) {
        try {
          if (this.cached == null) {
            this.cached = this.injector.getInstance(this.iface);
          }
        } catch (InjectionException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return this.cached;
  }

  @Override
  public T get(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

}