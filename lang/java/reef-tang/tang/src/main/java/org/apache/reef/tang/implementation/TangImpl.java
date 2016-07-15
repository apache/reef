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

import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;
import org.apache.reef.tang.implementation.java.InjectorImpl;
import org.apache.reef.tang.implementation.java.JavaConfigurationBuilderImpl;
import org.apache.reef.tang.implementation.types.NamedObjectImpl;
import org.apache.reef.tang.types.NamedObject;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.*;

public class TangImpl implements Tang {

  private static Map<SetValuedKey, JavaClassHierarchy> defaultClassHierarchy = new HashMap<>();

  /**
   * Only for testing. Deletes Tang's current database of known classes, forcing
   * it to rebuild them over time.
   */
  public static void reset() {
    defaultClassHierarchy = new HashMap<>();
  }

  @Override
  public Injector newInjector(final Configuration... confs) throws BindException {
    return new InjectorImpl(new JavaConfigurationBuilderImpl(confs).build());
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0], new Class[0]);
    } catch (final BindException e) {
      throw new IllegalStateException(
          "Caught unexpected bind exception! Implementation bug.", e);
    }
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(final ClassHierarchy ch) {
    return new ConfigurationBuilderImpl(ch);
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(final URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0], new Class[0]);
    } catch (final BindException e) {
      throw new IllegalStateException(
          "Caught unexpected bind exception! Implementation bug.", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(
      final Configuration... confs) throws BindException {
    return newConfigurationBuilder(new URL[0], confs, new Class[0]);
  }

  @Override
  public final JavaConfigurationBuilder newConfigurationBuilder(
      @SuppressWarnings("unchecked") final Class<? extends ExternalConstructor<?>>... parsers) throws BindException {
    return newConfigurationBuilder(new URL[0], new Configuration[0], parsers);
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(final URL[] jars, final Configuration[] confs,
          final Class<? extends ExternalConstructor<?>>[] parameterParsers)
      throws BindException {
    return new JavaConfigurationBuilderImpl(jars, confs, parameterParsers);
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaClassHierarchy getDefaultClassHierarchy() {
    return getDefaultClassHierarchy(new URL[0], new Class[0]);
  }

  @Override
  public JavaClassHierarchy getDefaultClassHierarchy(final URL[] jars,
                                                     final Class<? extends ExternalConstructor<?>>[] parameterParsers) {
    final SetValuedKey key = new SetValuedKey(jars, parameterParsers);

    JavaClassHierarchy ret = defaultClassHierarchy.get(key);
    if (ret == null) {
      ret = new ClassHierarchyImpl(jars, parameterParsers);
      defaultClassHierarchy.put(key, ret);
    }
    return ret;
  }

  @Override
  public Injector newInjector(final Configuration confs) {
    try {
      return newInjector(new Configuration[]{confs});
    } catch (final BindException e) {
      throw new IllegalStateException("Unexpected error cloning configuration", e);
    }
  }

  @Override
  public Injector newInjector() {
    try {
      return newInjector(new Configuration[]{});
    } catch (final BindException e) {
      throw new IllegalStateException("Unexpected error from empty configuration", e);
    }
  }

  @Override
  public <T> NamedObject newNamedObject(final Class<T> namedObjectType, final String name) {
    if (namedObjectType.isInterface() || Modifier.isAbstract(namedObjectType.getModifiers())) {
      throw new IllegalArgumentException("Every NamedObject should have a concrete class type.");
    }
    return new NamedObjectImpl<>(namedObjectType, name);
  }

  private class SetValuedKey {
    private final Set<Object> key;

    SetValuedKey(final Object[] ts, final Object[] us) {
      key = new HashSet<>(Arrays.asList(ts));
      key.addAll(Arrays.asList(us));
    }

    @Override
    public int hashCode() {
      int i = 0;
      for (final Object t : key) {
        i += t.hashCode();
      }
      return i;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SetValuedKey other = (SetValuedKey) o;
      if (other.key.size() != this.key.size()) {
        return false;
      }
      return key.containsAll(other.key);
    }
  }


}
