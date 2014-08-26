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

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;

/**
 * Immutable, type-checked configuration data. 
 * 
 * Tang Configuration objects are constructed via
 * ConfigurationBuilders, and most applications interact with the
 * Configuration API much more than the one described here.  See
 * the ConfigurationBuilder documentation for a discussion of the 
 * semantics of configuration options.  The documentation provided
 * here is primarily for people that wish to extend Tang or implement
 * formats that export data from Configuration objects to other systems.
 * 
 * Conceptually, a configuration contains a set of key
 * value pairs.  Each pair either maps from an interface to an
 * implementation (a class) or from a configuration option to a
 * value (e.g., an integer or string).
 * 
 * Under the hood, Configuration objects carry much richer type
 * information than this, and also refer to the ClassHierarchy
 * object they were checked against.  Configurations can be 
 * merged into each other by creating a new ConfigurationBuilder
 * object, and passing in multiple configurations.  In such situations,
 * Tang automatically merges the reflection data from the underlying
 * ClassHierarchy objects, and re-validates the merged configuration
 * data against the merged classpath.
 * 
 * Note that the left hand side of each configuration object (the
 * "key" in the key value pair) is unique.  Although there are many
 * APIs that take NamedParameterNode objects in this API, a given
 * NamedParameterNode represents a unique type of binding, and is only
 * applicable to one of the getters below.  These APIs use Java generic
 * types to make it easier to map from NamedParameterNode to the appropriate
 * getter. 
 */
public interface Configuration {

  /**
   * Create a new ConfigurationBuilder object based on the same classpath
   * as this Configuration, and populate it with the configuration options
   * of this object.
   * 
   * This API is unstable and should be considered private.  Use the methods
   * in com.microsoft.reef.Tang instead.
   */
  public ConfigurationBuilder newBuilder();
  /**
   * Return the value of the given named parameter as an unparsed string.
   * 
   * If nothing was explicitly bound, this method returns null (it does not
   * return default values).
   * 
   * @param np A NamedParameter object from this Configuration's class hierarchy.  
   * @return The validated string that this parameter is bound to, or null.
   * @see getClassHierarchy()
   */
  public String getNamedParameter(NamedParameterNode<?> np);
  
  /**
   * Obtain the set of class hierarchy nodes or strings that were bound to a given NamedParameterNode.
   * If nothing was explicitly bound, the set will be empty (it will not reflect any default values).
   * @param np A NamedParameterNode from this Configuration's class hierarchy.
   * @return A set of ClassHierarchy Node objects or a set of strings, depending on whether the NamedParameterNode refers to an interface or configuration options, respectively.
   * @see getClassHierarchy()
   */
  public Set<Object> getBoundSet(NamedParameterNode<Set<?>> np);

  /**
   * Get the list bound to a given NamedParameterNode. The list will be empty if nothing was bound.
   *
   * @param np Target NamedParameter
   * @return A list bound to np
   */
  public List<Object> getBoundList(NamedParameterNode<List<?>> np);
  
  /**
   * @return the external constructor that cn has been explicitly bound to, or null.  Defaults are not returned.
   */
  public <T> ClassNode<ExternalConstructor<T>> getBoundConstructor(ClassNode<T> cn);
  /**
   * @return the implementation that cn has been explicitly bound to, or null.  Defaults are not returned.
   */
  public <T> ClassNode<T> getBoundImplementation(ClassNode<T> cn);
  /**
   * Return the LegacyConstructor that has been bound to this Class.  Such constructors are defined in the class, but missing their @Inject annotation.
   * 
   * For now, only one legacy constructor can be bound per class.
   * 
   * TODO: Should this return Set<ConstructorDef<T>> instead?
   */
  public <T> ConstructorDef<T> getLegacyConstructor(ClassNode<T> cn);
  /**
   * @return the set of all interfaces (or super-classes) that have been explicitly
   * bound to an implementation sub-class.
   */
  Set<ClassNode<?>> getBoundImplementations();
  /**
   * @return the set of all the interfaces that have had an external constructor bound to them.
   */
  Set<ClassNode<?>> getBoundConstructors();
  /**
   * @return the set of all the named parameters that have been explicitly bound to something.
   */
  Set<NamedParameterNode<?>> getNamedParameters();
  /**
   * @return the set of all interfaces that have a legacy constructor binding.
   */
  Set<ClassNode<?>> getLegacyConstructors();
  /**
   * Configuration objects are associated with the ClassHierarchy objects that were used during validation.
   *  
   * @return the ClassHierarchy that backs this Configuration.
   */
  public ClassHierarchy getClassHierarchy();
  /**
   * Get the set bindings from set-valued NamedParameters to the values they were bound to.  
   * 
   * TODO: This API seems wonky.  Why not return a Set<NamedParameterNode<Set<?>>> instead, and let the caller invoke getBoundSet() above?
   * 
   * @return a flattened set with one entry for each binding (the same NamedParameterNode may be a key for multiple bindings.
   *
   * @deprecated 
   */
  @Deprecated
  Iterable<Entry<NamedParameterNode<Set<?>>, Object>> getBoundSets();

  /**
   * @return the set of all NamedParameterNodes explicitly bound to lists.
   */
  Set<NamedParameterNode<List<?>>> getBoundLists();

}