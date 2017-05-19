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

import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.types.*;

import java.util.List;
import java.util.Set;

/**
 * This class allows applications to register bindings with Tang.  Tang
 * configurations are simply sets of bindings of various types.  The most
 * common bindings are of interfaces (or superclasses) to implementation
 * classes, and of configuration options ("NamedParameters") to values.
 * <p>
 * Implementations of this class type check the bindings against an underlying
 * ClassHierarchy implementation.  Typically, the backing ClassHierarchy will
 * be delegate to the default classloader (the one that loaded the code that is
 * invoking Tang), though other scenarios are possible.  For instance, the
 * ClassHierarchy could incorporate additional Jars, or it might not delegate
 * to the default classloader at all.  In fact, Tang supports ClassHierarchy
 * objects that are derived from reflection data from other languages, such as
 * C#.  This enables cross-language injection sessions, where Java code
 * configures C# code, or vice versa.
 * <p>
 * When possible, the methods in this interface eagerly check for these
 * errors.  Methods that check for configuration and other runtime or
 * application-level errors are declared to throw BindException.
 * <p>
 * Furthermore, all methods in Tang, may throw RuntimeException if they
 * encounter inconsistencies in the underlying ClassHierarchy.  Such errors
 * reflect problems that existed when the application was compiled or
 * packaged, and cannot be corrected at runtime.  Examples include
 * inconsistent type hierarchies (such as version mismatches between jars),
 * and unparsable default values (such as an int that defaults to "false"
 * or "five").  These exceptions are analogous to the runtime exceptions
 * thrown by the Java classloader; other than logging them or reporting them
 * to an end user, applications have little recourse when such problems are
 * encountered.
 *
 * @see JavaConfigurationBuilder for convenience methods that assume the
 * underlying ClassHierarchy object delegates to the default
 * classloader, and enable many compile time static checks.
 * @see org.apache.reef.tang.formats.ConfigurationModule which pushes additional type checks to class load
 * time.  This allows Tint, Tang's static analysis tool, to detect a wide
 * range of runtime configuration errors at build time.
 */
public interface ConfigurationBuilder {

  /**
   * Add all configuration parameters from the given Configuration object.
   *
   * @param c the configuration to be added
   */
  void addConfiguration(final Configuration c) throws BindException;

  /**
   * Each ConfigurationBuilder instance is associated with a ClassHierarchy.
   * It uses this ClassHierarchy to validate the configuration options that it
   * processes.
   *
   * @return a reference to the ClassHierarchy instance backing this
   * ConfigurationBuilder. No copy is made, since ClassHierarchy objects
   * are effectively immutable.
   */
  ClassHierarchy getClassHierarchy();

  /**
   * Force Tang to treat the specified constructor as though it had an @Inject
   * annotation.
   * <p>
   * This method takes ClassNode objects.  Like all of the methods in this
   * API, the ClassNode objects must come from the ClassHierarchy instance
   * returned by getClassHierarchy().
   *
   * @param cn   The class the constructor instantiates.
   * @param args The types of the arguments taken by the constructor, in declaration order.
   * @throws BindException if the constructor does not exist, or if it has already been bound as a legacy constructor.
   */
  void registerLegacyConstructor(ClassNode<?> cn, ClassNode<?>... args)
      throws BindException;

  /**
   * Force Tang to treat the specified constructor as though it had an @Inject
   * annotation.
   *
   * @param cn   The full name of the class the constructor instantiates.
   * @param args The full names of the types of the arguments taken by the constructor, in declaration order.
   * @throws BindException if the constructor does not exist, or if it has already been bound as a legacy constructor.
   */
  void registerLegacyConstructor(String cn, String... args)
      throws BindException;

  /**
   * Force Tang to treat the specified constructor as though it had an @Inject
   * annotation.
   * <p>
   * This method takes ClassNode and ConstructorArg objects.  Like all of the
   * methods in this API, these objects must come from the ClassHierarchy
   * instance returned by getClassHierarchy().
   *
   * @param c   The class the constructor instantiates.
   * @param args The parsed ConstructorArg objects corresponding to the types of the arguments taken by the constructor,
   *             in declaration order.
   * @throws BindException if the constructor does not exist, or if it has already been bound as a legacy constructor.
   */
  void registerLegacyConstructor(ClassNode<?> c, ConstructorArg... args)
      throws BindException;

  /**
   * Bind classes to each other, based on their full class names; alternatively,
   * bound a NamedParameter configuration option to a configuration value. You can
   * not bind a NamedObject using this method.
   *
   * @param <T> a type
   * @param iface The full name of the interface that should resolve to impl,
   *              or the NamedParameter to be set.
   * @param impl  The full name of the implementation that will be used in
   *              place of impl, or the value the NamedParameter should be set to.
   * @throws BindException If (In the case of interfaces and implementations)
   *                       the underlying ClassHierarchy does not recognise iface and
   *                       impl as known, valid classes, or if impl is not a in
   *                       implementation of iface, or (in the case of NamedParameters
   *                       and values) if iface is not a NamedParameter, or if impl
   *                       fails to parse as the type the iface expects.
   */
  <T> void bind(String iface, String impl)
      throws BindException;

  /**
   * Bind a NamedNodeObject to an interface / NamedParameter.
   *
   * @param iface The interface / NamedParameter to be bound
   * @param impl  The NamedNodeObject implementation to be bound
   * @throws BindException
   * @see bind(String, String) for a more complete description.
   *
   */
  <T> void bind(String iface, NamedObjectElement<T> impl) throws BindException;

  /**
   * Bind classes to each other, based on their full class names; alternatively,
   * bound a NamedParameter configuration option to a configuration value.
   * <p>
   * This method takes Node objects.  Like all of the methods in this API,
   * these objects must come from the ClassHierarchy instance returned by
   * getClassHierarchy().
   *
   * @param iface   The interface / NamedParameter to be bound.
   * @param impl The implementation / value iface should be set to.
   * @throws BindException if there is a type checking error
   * See {@link #bind(String, String) bind(String,String)} for a more complete description.
   */
  void bind(Node iface, Node impl) throws BindException;

  /**
   +   * Bind a NamedNodeObject to an interface / NamedParameter.
   +   * @param iface The interface / NamedParameter to be bound
   +   * @param impl The NamedNodeObject implementation to be bound
   +   * @throws BindException
   +   * @see bind(String, String) for a more complete description.
   +   */
  void bind(Node iface, NamedObjectElement impl) throws BindException;

  /**
   * Extension methods for bind() for supporting NamedObjects.
   */
  void bind(String iface, String impl, NamedObjectElement namedObjectElement) throws BindException;

  void bind(Node iface, Node impl, NamedObjectElement namedObjectElement) throws BindException;

  <T> void bind(String iface, NamedObjectElement<T> impl, NamedObjectElement namedObjectElement) throws BindException;

  void bind(Node iface, NamedObjectElement impl, NamedObjectElement namedObjectElement) throws BindException;

  /**
   * Register an ExternalConstructor implementation with Tang.
   * ExternalConstructors are proxy classes that instantiate some
   * other class.  They have two primary use cases: (1) adding new
   * constructors to classes that you cannot modify and (2) implementing
   * constructors that examine their arguments and return an instance of a
   * subclass of the requested object.
   * <p>
   * To see how the second use case could be useful, consider a implementing a
   * URI interface with a distinct subclass for each valid URI prefix (e.g.,
   * http://, ssh://, etc...).  An ExternalConstructor could examine the prefix
   * and delegate to a constructor of the correct implementation (e.g, HttpURL,
   * SshURL, etc...) which would validate the remainder of the provided string.
   * URI's external constructor would return the validated subclass of URI that
   * corresponds to the provided string, allowing instanceof and downcasts to
   * behave as expected in the code that invoked Tang.
   * <p>
   * Both use cases should be avoided when possible, since they can
   * unnecessarily complicate object injections and undermine Tang's ability
   * to statically check a given configuration.
   * <p>
   * This method takes ClassNode objects.  Like all of the methods in this API,
   * these objects must come from the ClassHierarchy instance returned by
   * getClassHierarchy().
   *
   * @param <T> a type
   * @param iface The class or interface to be instantiated.
   * @param impl  The ExternalConstructor class that will be used to instantiate iface.
   * @throws BindException If impl does not instantiate a subclass of iface.
   */
  <T> void bindConstructor(ClassNode<T> iface,
                           ClassNode<? extends ExternalConstructor<? extends T>> impl)
      throws BindException;

  /**
   * Pretty print the default implementation / value of the provided class / NamedParameter.
   * This is used by Tang to produce human readable error messages.
   *
   * @param longName a name of class or parameter
   * @return a pretty-formatted string for class or named parameter
   * @throws BindException if name resolution fails
   */
  String classPrettyDefaultString(String longName) throws BindException;

  /**
   * Pretty print the human readable documentation of the provided class / NamedParameter.
   * This is used by Tang to produce human readable error messages.
   *
   * @param longName a name of class or parameter
   * @return a pretty-formatted class description string
   * @throws BindException if name resolution fails
   */
  String classPrettyDescriptionString(String longName)
      throws BindException;

  /**
   * Produce an immutable Configuration object that contains the current
   * bindings and ClassHierarchy of this ConfigurationBuilder.  Future
   * changes to this ConfigurationBuilder will not be reflected in the
   * returned Configuration.
   * <p>
   * Since Tang eagerly checks for configuration errors, this method does not
   * perform any additional validation, and does not throw any checkable
   * exceptions.
   *
   * @return the Configuration
   */
  Configuration build();

  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, Node impl)
      throws BindException;

  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, String impl)
      throws BindException;

  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, NamedObjectElement<? extends T> impl)
      throws BindException;

  void bindSetEntry(String iface, String impl) throws BindException;

  void bindSetEntry(String iface, Node impl) throws BindException;

  void bindSetEntry(String iface, NamedObjectElement impl) throws BindException;

  /**
   * Extension methods for bindSetEntry() for supporting NamedObjects.
   */
  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, Node impl, NamedObjectElement namedObjectElement)
      throws BindException;

  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, String impl, NamedObjectElement namedObjectElement)
      throws BindException;

  <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, NamedObjectElement<? extends T> impl,
                        NamedObjectElement namedObjectElement) throws BindException;

  <T> void bindSetEntry(String iface, String impl, NamedObjectElement namedObjectElement) throws BindException;

  <T> void bindSetEntry(String iface, Node impl, NamedObjectElement namedObjectElement) throws BindException;

  <T> void bindSetEntry(String iface, NamedObjectElement impl, NamedObjectElement namedObjectElement)
      throws BindException;

  /**
   * Bind an list of implementations(Class or String) to an given NamedParameter.
   * Unlike bindSetEntry, bindListEntry will bind a whole list to the parameter,
   * not an element of the list.
   * <p>
   * Since ordering of the list is important, list binding cannot be repeated or
   * merged unlike set binding. If the elements of the list are Classes, the objects
   * created by the Classes will be injected. If the elements are Strings, the values
   * will be injected directly to a given list parameter. If the elements are NamedObjects,
   * independently configured named objects will be injected to a list.
   *
   * @param <T> a type
   * @param iface    The list named parameter to be instantiated
   * @param implList The list of class or value will be used to instantiated the named parameter
   * @throws BindException if implementation class does not fit to the named parameter
   */
  <T> void bindList(NamedParameterNode<List<T>> iface, List implList) throws BindException;

  void bindList(String iface, List implList) throws BindException;

  /**
   * Extension methods for bindList() for supporting NamedObjects.
   */

  <T> void bindList(NamedParameterNode<List<T>> iface, List implList,
                    NamedObjectElement namedObjectElement) throws BindException;

  <T> void bindList(String iface, List implList, NamedObjectElement namedObjectElement) throws BindException;
}
