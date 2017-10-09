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
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorDef;
import org.apache.reef.tang.types.NamedParameterNode;

import java.util.List;
import java.util.Set;

public class ConfigurationImpl implements Configuration {

  private ConfigurationBuilderImpl builder=null;

  public ConfigurationImpl(){}

  protected ConfigurationImpl(final ConfigurationBuilderImpl builder) {
    this.builder = builder;
  }

  @Override
  public String getNamedParameter(final NamedParameterNode<?> np) {
    return builder.namedParameters.get(np);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ClassNode<ExternalConstructor<T>> getBoundConstructor(
      final ClassNode<T> cn) {
    return (ClassNode<ExternalConstructor<T>>) builder.boundConstructors.get(cn);
  }

  @Override
  public Set<ClassNode<?>> getBoundImplementations() {
    return builder.boundImpls.keySet();
  }

  @Override
  public Set<ClassNode<?>> getBoundConstructors() {
    return builder.boundConstructors.keySet();
  }

  @Override
  public Set<NamedParameterNode<?>> getNamedParameters() {
    return builder.namedParameters.keySet();
  }

  @Override
  public Set<ClassNode<?>> getLegacyConstructors() {
    return builder.legacyConstructors.keySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ClassNode<T> getBoundImplementation(final ClassNode<T> cn) {
    return (ClassNode<T>) builder.boundImpls.get(cn);
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
  public Set<Object> getBoundSet(final NamedParameterNode<Set<?>> np) {
    return this.builder.boundSetEntries.getValuesForKey(np);
  }

  @Override
  public List<Object> getBoundList(final NamedParameterNode<List<?>> np) {
    return this.builder.boundLists.get(np);
  }

  @Override
  public Set<NamedParameterNode<Set<?>>> getBoundSets() {
    return builder.boundSetEntries.keySet();
  }

  @Override
  public Set<NamedParameterNode<List<?>>> getBoundLists() {
    return builder.boundLists.keySet();
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
}
