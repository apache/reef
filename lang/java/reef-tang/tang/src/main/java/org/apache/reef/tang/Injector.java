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
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.types.NamedObject;

public interface Injector {

  /**
   * Gets an instance of iface, or the implementation that has been bound to it.
   * If an instance has already been created in this (or a parent) scope, then
   * the existing instance will be returned. Otherwise, a new instance will be
   * returned, and registered in the current scope.
   *
   * @param <U> a type
   * @param iface an interface
   * @return an instance
   * @throws InjectionException if it fails to find corresponding class
   */
  <U> U getInstance(Class<U> iface) throws InjectionException;

  <U> U getInstance(Class<U> iface, NamedObject namedObject) throws InjectionException;

  <U> U getInstance(String iface, NamedObject namedObject) throws InjectionException, NameResolutionException;

  /**
   * Gets an instance of iface, or the implementation that has been bound to it.
   * If an instance has already been created in this (or a parent) scope, then
   * the existing instance will be returned. Otherwise, a new instance will be
   * returned, and registered in the current scope.
   *
   * @param <U> a type
   * @param iface a name of interface
   * @return an instance
   * @throws InjectionException if it fails to find corresponding class
   * @throws NameResolutionException if name resolution fails
   */
  <U> U getInstance(String iface) throws InjectionException,
      NameResolutionException;

  /**
   * Gets the value stored for the given named parameter.
   *
   * @param <U> a type
   * @param iface the name of interface
   * @return an Instance of the class configured as the implementation for the
   * given interface class.
   * @throws InjectionException if name resolution fails
   */
  <U> U getNamedInstance(Class<? extends Name<U>> iface, NamedObject namedObject)
      throws InjectionException;

  <U> U getNamedInstance(Class<? extends Name<U>> iface)
      throws InjectionException;

  /**
   * Gets an instance of the implementation that has been bound to the type of namedObject
   * in the space of namedObject.
   *
   * @param namedObject
   * @param <U>
   * @return an Instance of the NamedObject.
   * @throws InjectionException
   */
  <U> U getNamedObjectInstance(NamedObject<U> namedObject) throws InjectionException;

  /**
   * Binds the given object to the class. Note that this only affects objects
   * created by the returned Injector and its children. Also, like all
   * Injectors, none of those objects can be serialized back to a configuration
   * file).
   *
   * @param <T> a type
   * @param iface an interface cass
   * @param inst an instance
   * @throws BindException when trying to re-bind
   */
  <T> void bindVolatileInstance(Class<T> iface, T inst, NamedObject o)
      throws BindException;

  <T> void bindVolatileInstance(Class<T> iface, T inst)
      throws BindException;

  <T> void bindVolatileParameter(Class<? extends Name<T>> iface, T inst, NamedObject o)
      throws BindException;

  <T> void bindVolatileParameter(Class<? extends Name<T>> iface, T inst)
      throws BindException;

  /**
   * Binds a TANG Aspect to this injector.  Tang Aspects interpose on each
   * injection performed by an injector, and return an instance of their choosing.
   * <p>
   * A given aspect will be invoked once for each object that Tang injects, and aspects
   * will be copied in a way that mirrors the scoping that Tang creates at runtime.
   *
   * @param <T> a type
   * @param a aspect
   * @throws BindException if there exists a bound aspect already
   */
  <T> void bindAspect(Aspect a) throws BindException;

  /**
   * Allows InjectionFuture to tell the aspect when get() is invoked.  Package private.
   *
   * @return the aspect
   */
  Aspect getAspect();

  /**
   * Create a copy of this Injector that inherits the instances that were already
   * created by this Injector, but reflects additional Configuration objects.
   * This can be used to create trees of Injectors that obey hierarchical
   * scoping rules.
   * <p>
   * Except for the fact that the child Injector will have references to this
   * injector's instances, the returned Injector is equivalent to the one you
   * would get by using ConfigurationBuilder to build a merged Configuration,
   * and then using the merged Configuration to create an Injector. Injectors
   * returned by ConfigurationBuilders are always independent, and never
   * share references to the same instances of injected objects.
   *
   * @param configurations configurations
   * @return an injector
   * @throws BindException If any of the configurations conflict with each other, or the
   *                       existing Injector's Configuration.
   */
  Injector forkInjector(Configuration... configurations) throws BindException;

  /**
   * Returns true if this Injector is able to instantiate the object named by
   * name.
   *
   * @param name a name of object
   * @return whether the name is injectable
   * @throws BindException if there is a loop or the name is package name
   */
  boolean isInjectable(String name) throws BindException;

  boolean isInjectable(String name, NamedObject no) throws NameResolutionException;

  boolean isParameterSet(String name) throws BindException;

  boolean isParameterSet(String name, NamedObject no) throws NameResolutionException;

  boolean isInjectable(Class<?> clazz) throws BindException;

  boolean isInjectable(Class<?> clazz, NamedObject no) throws BindException;

  boolean isParameterSet(Class<? extends Name<?>> name) throws BindException;

  boolean isParameterSet(Class<? extends Name<?>> name, NamedObject no) throws BindException;

  InjectionPlan<?> getInjectionPlan(String name) throws NameResolutionException;

  InjectionPlan<?> getInjectionPlan(String name, NamedObject no) throws NameResolutionException;

  <T> InjectionPlan<T> getInjectionPlan(Class<T> name);

  Injector forkInjector();
}
