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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.implementation.java.InjectorImpl;
import org.apache.reef.tang.implementation.types.NamedObjectImpl;
import org.apache.reef.tang.types.NamedObjectElement;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A future-based mechanism for cyclic object injections. Since Tang is a
 * constructor-based dependency injector, there is no way to directly create
 * cycles of objects.
 * <p>
 * In situations where you need to have two objects that point at each other, you
 * can use an InjectionFuture to break the cycle. Simply ask Tang to inject an
 * {@code InjectionFuture<T>} into your constructor.  Later (after your constructor
 * returns) invoke the get() method of the injection future to get a reference
 * to an injected object of type T.
 * <p>
 * Note that InjectorFutures and singletons interact in subtle ways.
 * <p>
 * Normally, Tang never reuses a reference to an injected object unless the
 * object is a singleton or a volatile instance. If InjectionFutures followed
 * this convention, then a cyclic injection of two non-singleton classes would
 * result in an an infinite chain of objects of the two types. Tang addresses
 * this issue as follows:
 * <p>
 * 1) In the first pass, it injects a complete object tree, making note of
 * InjectionFuture objects that will need to be populated later. The injection
 * of this tree respects standard Tang singleton and volatile semantics.
 * <p>
 * 2) In a second pass, Tang populates each of the InjectionFutures with the
 * reference to the requested class that was instantiated in the first pass. If
 * this reference does not exist (or is non-unique) then an InjectionException
 * is thrown.
 * <p>
 * Note: The semantics of complex cyclic injections may change over time.
 * <p>
 * We haven't seen many complicated injections that involve cycles in practice.
 * A second approach would be to establish some scoping rules, so that each
 * InjectionFuture binds to the innermost matching parent in the InjectionPlan.
 * This would allow plans to inject multiple cycles involving distinct objects
 * of the same type.
 */

public final class InjectionFuture<T> implements Future<T> {

  protected final InjectorImpl injector;

  private final Class<? extends T> iface;

  private final NamedObjectElement noe;

  private final T instance;

  public InjectionFuture() {
    injector = null;
    iface = null;
    noe = null;
    instance = null;
  }

  public InjectionFuture(final Injector injector, final Class<? extends T> iface, final NamedObjectElement noe) {
    this.injector = (InjectorImpl) injector;
    this.iface = iface;
    this.noe = noe;
    this.instance = null;
  }

  public InjectionFuture(final T instance) {
    this.injector = null;
    this.iface = null;
    this.noe = null;
    this.instance = instance;
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get() {
    if (instance != null) {
      return instance;
    }
    try {
      synchronized (injector) {
        final T t;
        if (Name.class.isAssignableFrom(iface)) {
          // nullNamedObjectElement has been assigned
          if (!noe.isNull()) {
            t = injector.getNamedInstance((Class<Name<T>>) iface,
                new NamedObjectImpl(noe.getImplementationClass(), noe.getName()));
          } else {
            t = injector.getNamedInstance((Class<Name<T>>) iface);
          }
        } else {
          // nullNamedObjectElement has been assigned
          if (!noe.isNull()) {
            t = injector.getInstance(iface, new NamedObjectImpl(noe.getImplementationClass(), noe.getName()));
          } else {
            t = injector.getInstance(iface);
          }
        }
        final Aspect a = injector.getAspect();
        if (a != null) {
          a.injectionFutureInstantiated(this, t);
        }
        return t;
      }
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get(final long timeout, final TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

}
