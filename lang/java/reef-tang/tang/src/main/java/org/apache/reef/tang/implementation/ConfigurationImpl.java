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
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.implementation.types.NamedObjectElementImpl;
import org.apache.reef.tang.types.*;


import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ConfigurationImpl implements Configuration {
  private final ConfigurationBuilderImpl builder;

  protected ConfigurationImpl(final ConfigurationBuilderImpl builder) {
    this.builder = builder;
  }

  @Override
  public Object getNamedParameter(final NamedParameterNode<?> np, final NamedObjectElement noe) {
    return builder.namedParameters.containsKey(noe) ? builder.namedParameters.get(noe).get(np) : null;
  }

  @Override
  public Object getNamedParameter(final NamedParameterNode<?> np) {
    return getNamedParameter(np, builder.nullNamedObjectElement);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ClassNode<ExternalConstructor<T>> getBoundConstructor(
      final ClassNode<T> cn) {
    return (ClassNode<ExternalConstructor<T>>) builder.boundConstructors.get(cn);
  }

  @Override
  public Set<ClassNode<?>> getBoundImplementations(final NamedObjectElement noe) {

    return builder.boundImpls.containsKey(noe) ?
        builder.boundImpls.get(noe).keySet() : Collections.<ClassNode<?>>emptySet();
  }

  @Override
  public Set<ClassNode<?>> getBoundImplementations() {
    return getBoundImplementations(builder.nullNamedObjectElement);
  }

  @Override
  public Set<ClassNode<?>> getBoundConstructors() {
    return builder.boundConstructors.keySet();
  }

  @Override
  public Set<NamedParameterNode<?>> getNamedParameters(final NamedObjectElement noe) {
    return builder.namedParameters.containsKey(noe) ?
        builder.namedParameters.get(noe).keySet() : Collections.<NamedParameterNode<?>>emptySet();
  }

  @Override
  public Set<NamedParameterNode<?>> getNamedParameters() {
    return getNamedParameters(builder.nullNamedObjectElement);
  }

  @Override
  public Set<ClassNode<?>> getLegacyConstructors() {
    return builder.legacyConstructors.keySet();
  }

  @Override
  public Boundable getBoundImplementation(final ClassNode cn, final NamedObjectElement noe) {
    return builder.boundImpls.containsKey(noe) ? builder.boundImpls.get(noe).get(cn) : null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Boundable getBoundImplementation(final ClassNode cn) {
    return getBoundImplementation(cn, builder.nullNamedObjectElement);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ConstructorDef<T> getLegacyConstructor(final ClassNode<T> cn) {
    return (ConstructorDef<T>) builder.legacyConstructors.get(cn);
  }

  @Override
  public ConfigurationBuilder newBuilder() {
    return builder.build().builder;
  }

  @Override
  public ClassHierarchy getClassHierarchy() {
    return builder.getClassHierarchy();
  }

  @Override
  public Set<Object> getBoundSet(final NamedParameterNode<Set<?>> np, final NamedObjectElement noe) {
    return builder.boundSetEntries.containsKey(noe) ?
        builder.boundSetEntries.get(noe).getValuesForKey(np) : Collections.emptySet();
  }

  @Override
  public Set<Object> getBoundSet(final NamedParameterNode<Set<?>> np) {
    return getBoundSet(np, builder.nullNamedObjectElement);
  }

  @Override
  public List<Object> getBoundList(final NamedParameterNode<List<?>> np, final NamedObjectElement noe) {
    return builder.boundLists.containsKey(noe) ? builder.boundLists.get(noe).get(np) : null;
  }

  @Override
  public List<Object> getBoundList(final NamedParameterNode<List<?>> np) {
    return getBoundList(np, builder.nullNamedObjectElement);
  }

  @Override
  public Set<NamedParameterNode<Set<?>>> getBoundSets(final NamedObjectElement noe) {
    return builder.boundSetEntries.containsKey(noe) ?
        builder.boundSetEntries.get(noe).keySet() : Collections.<NamedParameterNode<Set<?>>>emptySet();
  }

  @Override
  public Set<NamedParameterNode<Set<?>>> getBoundSets() {
    return getBoundSets(builder.nullNamedObjectElement);
  }


  @Override
  public Set<NamedParameterNode<List<?>>> getBoundLists(final NamedObjectElement noe) {
    return builder.boundLists.containsKey(noe) ?
        builder.boundLists.get(noe).keySet() : Collections.<NamedParameterNode<List<?>>>emptySet();
  }

  @Override
  public Set<NamedParameterNode<List<?>>> getBoundLists() {
    return getBoundLists(builder.nullNamedObjectElement);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ConfigurationImpl that = (ConfigurationImpl) o;

    if (builder != null ? !builder.equals(that.builder) : that.builder != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return builder != null ? builder.hashCode() : 0;
  }

  public ConfigurationBuilderImpl getBuilder() {
    return builder;
  }

  @Override
  public <T> NamedObjectElement<T> getNamedObjectElement(final NamedObject<T> no) {
    if (no == null) {
      return builder.nullNamedObjectElement;
    }
    final Node n = builder.getClassHierarchy().getNode(no.getType().getName());
    if (n instanceof ClassNode) {
      return new NamedObjectElementImpl((ClassNode) n, no.getType(), no.getName(), false);
    } else {
      throw new IllegalArgumentException("Internal error: Invalid type in NamedObject!");
    }
  }

  @Override
  public Set<NamedObjectElement> getNamedObjectElements() {
    return builder.namedObjectElements;
  }
}
