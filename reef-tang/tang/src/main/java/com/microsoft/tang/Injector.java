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
package com.microsoft.tang;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.InjectionPlan;

public interface Injector {

  /**
   * Gets an instance of iface, or the implementation that has been bound to it.
   * If an instance has alread been created in this (or a parent) scope, then
   * the existing instance will be returned. Otherwise, a new instance will be
   * returned, and registered in the current scope.
   *
   * @param iface
   * @return
   * @throws NameResolutionException
   * @throws ReflectiveOperationException
   */
  public <U> U getInstance(Class<U> iface) throws InjectionException;

  public <U> U getInstance(String iface) throws InjectionException,
      NameResolutionException;

  /**
   * Gets the value stored for the given named parameter.
   *
   * @param <U>
   * @param name
   * @return an Instance of the class configured as the implementation for the
   * given interface class.
   * @throws InjectionException
   */
  public <U> U getNamedInstance(Class<? extends Name<U>> iface)
      throws InjectionException;

  /**
   * Binds the given object to the class. Note that this only affects objects
   * created by the returned Injector and its children. Also, like all
   * Injectors, none of those objects can be serialized back to a configuration
   * file).
   *
   * @param iface
   * @param inst
   * @return A copy of this injector that reflects the new binding.
   * @throws BindException
   */
  public <T> void bindVolatileInstance(Class<T> iface, T inst)
      throws BindException;

  public <T> void bindVolatileParameter(Class<? extends Name<T>> iface, T inst)
      throws BindException;

  /**
   * Binds a TANG Aspect to this injector.  Tang Aspects interpose on each
   * injection performed by an injector, and return an instance of their choosing.
   * <p/>
   * A given aspect will be invoked once for each object that Tang injects, and aspects
   * will be copied in a way that mirrors the scoping that Tang creates at runtime.
   *
   * @param a
   * @throws BindException
   */
  public <T> void bindAspect(Aspect a) throws BindException;

  /**
   * Allows InjectionFuture to tell the aspect when get() is invoked.  Package private.
   *
   * @return
   */
  Aspect getAspect();

  /**
   * Create a copy of this Injector that inherits the instances that were already
   * created by this Injector, but reflects additional Configuration objects.
   * This can be used to create trees of Injectors that obey hierarchical
   * scoping rules.
   * <p/>
   * <p/>
   * Except for the fact that the child Injector will have references to this
   * injector's instances, the returned Injector is equivalent to the one you
   * would get by using ConfigurationBuilder to build a merged Configuration,
   * and then using the merged Configuration to create an Injector. Injectors
   * returned by ConfigurationBuilders are always independent, and never
   * share references to the same instances of injected objects.
   *
   * @throws BindException If any of the configurations conflict with each other, or the
   *                       existing Injector's Configuration.
   */
  public Injector forkInjector(Configuration... configurations) throws BindException;

  /**
   * Returns true if this Injector is able to instantiate the object named by
   * name.
   *
   * @param name
   * @return
   * @throws BindException
   */
  boolean isInjectable(String name) throws BindException;

  boolean isParameterSet(String name) throws BindException;

  boolean isInjectable(Class<?> clazz) throws BindException;

  boolean isParameterSet(Class<? extends Name<?>> name) throws BindException;

  InjectionPlan<?> getInjectionPlan(String name) throws NameResolutionException;

  <T> InjectionPlan<T> getInjectionPlan(Class<T> name);

  Injector forkInjector();
}
