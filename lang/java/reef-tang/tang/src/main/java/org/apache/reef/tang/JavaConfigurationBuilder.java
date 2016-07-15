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
import org.apache.reef.tang.types.NamedObject;

import java.util.List;
import java.util.Set;

/**
 * Convenience methods that extend the ConfigurationBuilder but assume that
 * the underlying ClassHierarchy delegates to the default Java classloader.
 * <p/>
 * In addition to being less verbose, this interface expresses many of Tang's
 * type checks in Java's generic type system.  This improves IDE
 * auto-completion.  It also allows the errors to be caught at compile time
 * instead of later on in the build process, or at runtime.
 *
 * @see org.apache.reef.tang.formats.ConfigurationModule which pushes additional type checks to class load
 * time.  This allows Tint, Tang's static analysis tool, to detect a wide
 * range of runtime configuration errors at build time.
 */
public interface JavaConfigurationBuilder extends ConfigurationBuilder {

  /**
   * Bind named parameters, implementations or external constructors, depending
   * on the types of the classes passed in.
   *
   * @param <T> a type
   * @param iface an interface class
   * @param impl an implementation class
   * @return the configuration builder
   */
  <T> JavaConfigurationBuilder bind(Class<T> iface, Class<?> impl) throws BindException;

  /**
   * Bind named parameters or implementations to a named object depending on the types of the classes
   * passed in.
   *
   * @param iface
   * @param impl
   *
   */
  <T> JavaConfigurationBuilder bind(Class<T> iface, NamedObject<?> impl) throws BindException;

  /**
   * Extension methods for supporting NamedObject for bind().
   */
  <T> JavaConfigurationBuilder bind(Class<T> iface, Class<?> impl, NamedObject namedObject) throws BindException;

  <T> JavaConfigurationBuilder bind(Class<T> iface, NamedObject<?> impl, NamedObject namedObject)
      throws BindException;


  /**
   * Binds the Class impl as the implementation of the interface iface.
   *
   * @param <T> a type
   * @param iface an interface class
   * @param impl  an implementation class
   * @return the configuration builder
   */
  <T> JavaConfigurationBuilder bindImplementation(Class<T> iface, Class<? extends T> impl)
      throws BindException;

  /**
   * Binds the NamedObject impl as the implementation of the interface iface.
   */

  <T> JavaConfigurationBuilder bindImplementation(Class<T> iface, NamedObject<? extends T> impl)
      throws BindException;

  /**
   * Extension methods for supporting NamedObject for bindImplementation().
   */
  <T> JavaConfigurationBuilder bindImplementation(Class<T> iface, Class<? extends T> impl,
                                                  NamedObject namedObject) throws BindException;

  <T> JavaConfigurationBuilder bindImplementation(Class<T> iface, NamedObject<? extends T> impl,
                                                  NamedObject namedObject) throws BindException;

  /**
   * Set the value of a named parameter.
   *
   * @param name  The dummy class that serves as the name of this parameter.
   * @param value A string representing the value of the parameter. Reef must know
   *              how to parse the parameter's type.
   * @return the configuration builder
   * @throws org.apache.reef.tang.exceptions.NameResolutionException which occurs when name resolution fails
   */
  JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<?>> name, String value)
      throws BindException;

  /**
   * Binds the Class impl as the implementation of the named parameter iface.
   */
  <T> JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<T>> iface,
                                                  Class<? extends T> impl) throws BindException;

  /**
   * Binds the NamedObject impl as the implementation of the named parameter iface.
   */
  <T> JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<T>> iface,
                                                  NamedObject<? extends T> impl) throws BindException;

  /**
   * Extension methods for supporting NamedObject for bindNamedParameter().
   */
  JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<?>> name, String value, NamedObject namedObject);

  <T> JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<T>> iface,
                                                  Class<? extends T> impl, NamedObject namedObject)
      throws BindException;

  <T> JavaConfigurationBuilder bindNamedParameter(Class<? extends Name<T>> iface,
                                                  NamedObject<? extends T> impl, NamedObject namedObject)
      throws BindException;

  /**
   * Bind external constructors for concrete classes used for instance instantiation.
   * @param c a class
   * @param v an external constructor
   * @param <T> a type of the target class
   * @return
   * @throws BindException
   */
  <T> JavaConfigurationBuilder bindConstructor(Class<T> c,
                                               Class<? extends ExternalConstructor<? extends T>> v)
      throws BindException;

  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, String value) throws BindException;

  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl)
      throws BindException;

  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, NamedObject<? extends T> impl)
      throws BindException;

  /**
   * Extension methods for supporting NamedObject for bindSetEntry().
   */
  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, String value,
                                            NamedObject namedObject) throws BindException;

  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl,
                                            NamedObject namedObject) throws BindException;

  <T> JavaConfigurationBuilder bindSetEntry(Class<? extends Name<Set<T>>> iface, NamedObject<? extends T> impl,
                                            NamedObject namedObject) throws BindException;

  /**
   * Binds a specific list to a named parameter. List's elements can be string values or class implementations.
   * Their type would be checked in this method. If their types are not applicable to the named parameter,
   * it will make an exception.
   *
   * @param iface The target named parameter to be injected into
   * @param impl  A concrete list
   * @param <T> A type
   * @return the configuration builder
   * @throws BindException It occurs when iface is not a named parameter or impl is not compatible to bind
   */
  <T> JavaConfigurationBuilder bindList(Class<? extends Name<List<T>>> iface, List impl)
      throws BindException;

  /**
   * An extension method for supporting NamedObject for bindList().
   */
  <T> JavaConfigurationBuilder bindList(Class<? extends Name<List<T>>> iface, List impl, NamedObject namedObject)
      throws BindException;
}
