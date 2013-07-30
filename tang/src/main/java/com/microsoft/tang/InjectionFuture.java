package com.microsoft.tang;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.implementation.java.InjectorImpl;

/**
 * A future-based mechanism for cyclic object injections. Since Tang is a
 * constructor-based dependency injector, there is no way to directly create
 * cycles of objects.
 * 
 * In situations where you need to have two objects that point at each other, you
 * can use an InjectionFuture to break the cycle. Simply ask Tang to inject an
 * InjectionFuture<T> into your constructor.  Later (after your constructor
 * returns) invoke the get() method of the injection future to get a reference
 * to an injected object of type T.
 * 
 * Note that InjectorFutures and singletons interact in subtle ways.
 * 
 * Normally, Tang never reuses a reference to an injected object unless the
 * object is a singleton or a volatile instance. If InjectionFutures followed
 * this convention, then a cyclic injection of two non-singleton classes would
 * result in an an infinite chain of objects of the two types. Tang addresses
 * this issue as follows:
 * 
 * 1) In the first pass, it injects a complete object tree, making note of
 * InjectionFuture objects that will need to be populated later. The injection
 * of this tree respects standard Tang singleton and volatile semantics.
 * 
 * 2) In a second pass, Tang populates each of the InjectionFutures with the
 * reference to the requested class that was instantiated in the first pass. If
 * this reference does not exist (or is non-unique) then an InjectionException
 * is thrown.
 * 
 * Note: The semantics of complex cyclic injections may change over time.
 * 
 * We haven't seen many complicated injections that involve cycles in practice.
 * A second approach would be to establish some scoping rules, so that each
 * InjectionFuture binds to the innermost matching parent in the InjectionPlan.
 * This would allow plans to inject multiple cycles involving distinct objects
 * of the same type.
 * 
 * @param <T>
 */

public final class InjectionFuture<T> implements Future<T> {

  protected final InjectorImpl injector;

  private final Class<? extends T> iface;
  
  private T cached = null;
  public InjectionFuture(final T cached) {
    injector = null;
    iface = null;
    this.cached = cached;
  }
  public InjectionFuture(final Injector injector, Class<? extends T> iface) {
    this.injector = (InjectorImpl)injector;
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
  public boolean isCached() {
    return this.cached != null;
  }
  public void set(T t) {
    if(this.cached != null) {
      throw new IllegalStateException("Attempt to double set future!");
    }
    this.cached = t;
  }

  @Override
  public T get(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

}