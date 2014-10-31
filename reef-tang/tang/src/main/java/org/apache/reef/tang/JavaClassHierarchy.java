/**
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

import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.ParseException;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;

public interface JavaClassHierarchy extends ClassHierarchy {
  /**
   * Look up a class object in this ClassHierarchy. Unlike the version that
   * takes a string in ClassHierarchy, this version does not throw
   * NameResolutionException.
   * <p/>
   * The behavior of this method is undefined if the provided Class object is
   * not from the ClassLoader (or an ancestor of the ClassLoader) associated
   * with this JavaClassHierarchy. By default, Tang uses the default runtime
   * ClassLoader as its root ClassLoader, so static references (expressions like
   * getNode(Foo.class)) are safe.
   *
   * @param c The class to be looked up in the class hierarchy.
   * @return The associated NamedParameterNode or ClassNode.
   */
  public Node getNode(Class<?> c);

  public Class<?> classForName(String name) throws ClassNotFoundException;

  /**
   * Parse a string value that has been passed into a named parameter.
   *
   * @param name  The named parameter that will receive the value.
   * @param value A string value to be validated and parsed.
   * @return An instance of T, or a ClassNode<? extends T>.
   * @throws ParseException if the value failed to parse, or parsed to the
   *                        wrong type (such as when it specifies a class that does not implement
   *                        or extend T).
   */
  public <T> T parse(NamedParameterNode<T> name, String value) throws ParseException;

  /**
   * Obtain a parsed instance of the default value of a named parameter
   *
   * @param name The named parameter that should be checked for a default instance.
   * @return The default instance or null, unless T is a set type.  If T is a set,
   * then this method returns a (potentially empty) set of default instances.
   * @throws ClassHierarchyException if an instance failed to parse.
   */
  public <T> T parseDefaultValue(NamedParameterNode<T> name) throws ClassHierarchyException;

}
