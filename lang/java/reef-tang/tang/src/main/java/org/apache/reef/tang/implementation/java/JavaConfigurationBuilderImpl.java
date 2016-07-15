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
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.JavaClassHierarchy;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.implementation.ConfigurationBuilderImpl;
import org.apache.reef.tang.implementation.ConfigurationImpl;
import org.apache.reef.tang.implementation.types.NamedObjectElementImpl;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JavaConfigurationBuilderImpl extends ConfigurationBuilderImpl
    implements JavaConfigurationBuilder {

  public JavaConfigurationBuilderImpl(final URL[] jars,
                                      final Configuration[] confs,
                                      final Class<? extends ExternalConstructor<?>>[] parsers)
      throws BindException {
    super(jars, confs, parsers);
  }

  JavaConfigurationBuilderImpl() {
    super();
  }

  public JavaConfigurationBuilderImpl(final URL[] jars) throws BindException {
    super(jars);
  }

  JavaConfigurationBuilderImpl(final JavaConfigurationBuilderImpl impl) {
    super(impl);
  }

  public JavaConfigurationBuilderImpl(final Configuration[] confs)
      throws BindException {
    super(confs);
  }

  @Override
  public ConfigurationImpl build() {
    return new JavaConfigurationImpl(new JavaConfigurationBuilderImpl(this));
  }

  private Node getNode(final Class<?> c) {
    return ((JavaClassHierarchy) getClassHierarchy()).getNode(c);
  }

  private <T> NamedObjectElement<T> getNamedObjectElement(final NamedObject<T> namedObject) {
    if (namedObject == null) {
      return new NamedObjectElementImpl(null, null, null, true);
    }
    final Node n = getNode(namedObject.getType());
    if (n instanceof ClassNode) {
      return new NamedObjectElementImpl((ClassNode) n, namedObject.getType(), namedObject.getName(), false);
    } else {
      throw new IllegalArgumentException("Internal error: Invalid type in NamedObject!");
    }
  }

  @Override
  public <T> JavaConfigurationBuilder bind(final Class<T> c, final Class<?> val,
                                           final NamedObject namedObject) throws BindException {
    super.bind(getNode(c), getNode(val), getNamedObjectElement(namedObject));
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bind(final Class<T> c, final Class<?> val) throws BindException {
    bind(c, val, null);
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bind(final Class<T> c, final NamedObject<?> val,
                                           final NamedObject namedObject) throws BindException {
    super.bind(getNode(c), (NamedObjectElement) getNamedObjectElement(val), getNamedObjectElement(namedObject));
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bind(final Class<T> c, final NamedObject<?> val) throws BindException {
    bind(c, val, null);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> JavaConfigurationBuilder bindImplementation(final Class<T> c, final Class<? extends T> d,
                                                         final NamedObject namedObject) throws BindException {
    final Node cn = getNode(c);
    final Node dn = getNode(d);
    if (!(cn instanceof ClassNode)) {
      throw new BindException(
          "bindImplementation passed interface that resolved to " + cn
              + " expected a ClassNode<?>");
    }
    if (!(dn instanceof ClassNode)) {
      throw new BindException(
          "bindImplementation passed implementation that resolved to " + dn
              + " expected a ClassNode<?>");
    }
    super.bindImplementation((ClassNode<T>) cn, (ClassNode<? extends T>) dn, getNamedObjectElement(namedObject));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> JavaConfigurationBuilder bindImplementation(final Class<T> c, final NamedObject<? extends T> impl,
                                                         final NamedObject namedObject) throws BindException {
    final Node cn = getNode(c);
    if (!(cn instanceof ClassNode)) {
      throw new BindException(
          "bindImplementation passed interface that resolved to " + cn
              + " expected a ClassNode<?>");
    }

    if (!c.isAssignableFrom(impl.getType())) {
      throw new BindException("Type mismatch between NamedParameter and NamedObject occured! "
              + impl.getType().toString() + "Cannot be assigned to " + c.toString());
    }
    super.bindImplementation((ClassNode<T>) cn, (NamedObjectElement) getNamedObjectElement(impl),
        getNamedObjectElement(namedObject));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> JavaConfigurationBuilder bindImplementation(final Class<T> c, final Class<? extends T> d)
      throws BindException {
    bindImplementation(c, d, null);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> JavaConfigurationBuilder bindImplementation(final Class<T> c, final NamedObject<? extends T> d)
      throws BindException {
    bindImplementation(c, d, null);
    return this;
  }

  @Override
  public JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<?>> name, final String value,
                                                     final NamedObject namedObject)
      throws BindException {
    if (value == null) {
      throw new IllegalStateException("The value null set to the named parameter is illegal: " + name);
    }
    final Node np = getNode(name);
    if (np instanceof NamedParameterNode) {
      super.bindParameter((NamedParameterNode<?>) np, value, getNamedObjectElement(namedObject));
      return this;
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + np);
    }
  }

  @Override
  public <T> JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<T>> iface,
                                                         final Class<? extends T> impl,
                                                         final NamedObject namedObject) throws BindException {
    final Node ifaceN = getNode(iface);
    if (!(ifaceN instanceof NamedParameterNode)) {
      throw new BindException("Type mismatch when setting named parameter " + ifaceN
          + " Expected NamedParameterNode");
    }
    bind(iface, impl, namedObject);
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<T>> iface,
                                                         final NamedObject<? extends T> impl,
                                                         final NamedObject namedObject) throws BindException {
    final Node ifaceN = getNode(iface);
    if (!(ifaceN instanceof NamedParameterNode)) {
      throw new BindException("Type mismatch when setting named parameter " + ifaceN
          + " Expected NamedParameterNode");
    }
    final Class<T> npClass =
            (Class<T>) ReflectionUtilities.getRawClass(ReflectionUtilities.getInterfaceTarget(Name.class, iface));
    if (!npClass.isAssignableFrom(impl.getType())) {
      throw new BindException("Type mismatch between NamedParameter and NamedObject occured! "
          + impl.getType().toString() + "Cannot be assigned to " + npClass.toString());
    }
    bind(iface, impl, namedObject);
    return this;
  }

  @Override
  public JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<?>> name, final String value)
      throws BindException {
    bindNamedParameter(name, value, null);
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<T>> iface,
                                                         final Class<? extends T> impl) throws BindException {
    bindNamedParameter(iface, impl, null);
    return this;
  }

  @Override
  public <T> JavaConfigurationBuilder bindNamedParameter(final Class<? extends Name<T>> iface,
                                                         final NamedObject<? extends T> impl) {
    bindNamedParameter(iface, impl, null);
    return this;
  }

  @SuppressWarnings({"unchecked"})
  public <T> JavaConfigurationBuilder bindConstructor(final Class<T> c,
                                                      final Class<? extends ExternalConstructor<? extends T>> v)
      throws BindException {
    final Node n = getNode(c);
    final Node m = getNode(v);
    if (!(n instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + n + "; expected ClassNode");
    }
    if (!(m instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + m + "; expected ClassNode");
    }
    super.bindConstructor((ClassNode<T>) n, (ClassNode<? extends ExternalConstructor<? extends T>>) m);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface, final String value,
                                                   final NamedObject namedObject) throws BindException {
    final Node n = getNode(iface);

    if (!(n instanceof NamedParameterNode)) {
      throw new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    final Type setType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if (!ReflectionUtilities.getRawClass(setType).equals(Set.class)) {
      throw new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
    }
//    Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
    super.bindSetEntry((NamedParameterNode<Set<T>>) n, value, getNamedObjectElement(namedObject));
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                   final Class<? extends T> impl,
                                                   final NamedObject namedObject) throws BindException {
    final Node n = getNode(iface);
    final Node m = getNode(impl);

    if (!(n instanceof NamedParameterNode)) {
      throw new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    final Type setType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if (!ReflectionUtilities.getRawClass(setType).equals(Set.class)) {
      throw new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
    }
    final Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
    if (!ReflectionUtilities.getRawClass(valType).isAssignableFrom(impl)) {
      throw new BindException("BindSetEntry got implementation " + impl +
          " that is incompatible with expected type " + valType);
    }

    super.bindSetEntry((NamedParameterNode<Set<T>>) n, m, getNamedObjectElement(namedObject));
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                   final NamedObject<? extends T> impl,
                                                   final NamedObject namedObject) throws BindException {
    final Node n = getNode(iface);

    if (!(n instanceof NamedParameterNode)) {
      throw new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    final Type setType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if (!ReflectionUtilities.getRawClass(setType).equals(Set.class)) {
      throw new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
    }
    final Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
    if (!ReflectionUtilities.getRawClass(valType).isAssignableFrom(impl.getType())) {
      throw new BindException("BindSetEntry got implementation " + impl +
          " that is incompatible with expected type " + valType);
    }

    super.bindSetEntry((NamedParameterNode<Set<T>>) n, getNamedObjectElement(impl), getNamedObjectElement(namedObject));
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface, final String value)
      throws BindException {
    bindSetEntry(iface, value, null);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                   final Class<? extends T> impl) throws BindException {
    bindSetEntry(iface, impl, null);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                   final NamedObject<? extends T> impl) throws BindException {
    bindSetEntry(iface, impl, null);
    return this;
  }

  /**
   * Binding list method for JavaConfigurationBuilder. It checks the type of a given named parameter,
   * and whether all the list's elements can be applied to the named parameter. The elements' type
   * should be either java Class or String.
   * <p>
   * It does not check whether the list's String values can be parsed to T, like bindSetEntry.
   *
   * @param iface    target named parameter to be instantiated
   * @param implList implementation list used to instantiate the named parameter
   * @param <T>      type of the list
   * @return bound JavaConfigurationBuilder object
   * @throws BindException
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindList(final Class<? extends Name<List<T>>> iface, final List implList,
                                               final NamedObject namedObject) throws BindException {
    final Node n = getNode(iface);
    final List<Object> result = new ArrayList<>();

    if (!(n instanceof NamedParameterNode)) {
      throw new BindException("BindList got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    final Type listType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if (!ReflectionUtilities.getRawClass(listType).equals(List.class)) {
      throw new BindException("BindList got a NamedParameter that takes a " + listType + "; expected List<...>");
    }
    if (!implList.isEmpty()) {
      final Type valType = ReflectionUtilities.getInterfaceTarget(List.class, listType);
      for (final Object item : implList) {
        if (item instanceof Class) {
          if (!ReflectionUtilities.getRawClass(valType).isAssignableFrom((Class) item)) {
            throw new BindException("BindList got a list element which is not assignable to the given Type; " +
                "expected: " + valType);
          }
          result.add(getNode((Class) item));
        } else if (item instanceof NamedObject) {
          final NamedObject<? extends T> namedObjectItem = (NamedObject) item;
          final Class<? extends T> implType = namedObjectItem.getType();
          if(!ReflectionUtilities.getRawClass(valType).isAssignableFrom(implType)) {
            throw new BindException("BindList got a list element which is not assignable to the given Type; " +
                "expected: " + valType);
          }
          result.add(getNamedObjectElement(namedObjectItem));
        } else if (item instanceof String) {
          result.add(item);
        } else {
          throw new BindException("BindList got an list element with unsupported type; expected Class or String " +
              "object");
        }
      }
    }

    super.bindList((NamedParameterNode<List<T>>) n, result, getNamedObjectElement(namedObject));
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> JavaConfigurationBuilder bindList(final Class<? extends Name<List<T>>> iface, final List implList)
      throws BindException {
    bindList(iface, implList, null);
    return this;
  }

  private class JavaConfigurationImpl extends ConfigurationImpl {
    JavaConfigurationImpl(final JavaConfigurationBuilderImpl builder) {
      super(builder);
    }
  }
}
