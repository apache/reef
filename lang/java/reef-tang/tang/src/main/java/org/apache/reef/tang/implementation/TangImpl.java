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
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;
import org.apache.reef.tang.implementation.java.InjectorImpl;
import org.apache.reef.tang.implementation.java.JavaConfigurationBuilderImpl;

import java.net.URL;
import java.util.*;

public class TangImpl implements Tang {

  private static Map<SetValuedKey, JavaClassHierarchy> defaultClassHierarchy = new HashMap<>();

  /**
   * Only for testing. Deletes Tang's current database of known classes, forcing
   * it to rebuild them over time.
   */
  public static void reset() {
    defaultClassHierarchy = new HashMap<>(); //new ClassHierarchyImpl();
  }

  @Override
  public Injector newInjector(Configuration... confs) throws BindException {
    return new InjectorImpl(new JavaConfigurationBuilderImpl(confs).build());
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0], new Class[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public ConfigurationBuilder newConfigurationBuilder(ClassHierarchy ch) {
    return new ConfigurationBuilderImpl(ch);
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0], new Class[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(
      Configuration... confs) throws BindException {
    return newConfigurationBuilder(new URL[0], confs, new Class[0]);
  }

  @Override
  public final JavaConfigurationBuilder newConfigurationBuilder(
      @SuppressWarnings("unchecked") Class<? extends ExternalConstructor<?>>... parsers) throws BindException {
    return newConfigurationBuilder(new URL[0], new Configuration[0], parsers);
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
                                                          Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parameterParsers) throws BindException {
    JavaConfigurationBuilder cb = new JavaConfigurationBuilderImpl(jars, confs, parameterParsers);
//    for (Configuration c : confs) {
//      cb.addConfiguration(c);
//    }
    return cb;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaClassHierarchy getDefaultClassHierarchy() {
    return getDefaultClassHierarchy(new URL[0], new Class[0]);
  }

  @Override
  public JavaClassHierarchy getDefaultClassHierarchy(URL[] jars, Class<? extends ExternalConstructor<?>>[] parameterParsers) {
    SetValuedKey key = new SetValuedKey(jars, parameterParsers);

    JavaClassHierarchy ret = defaultClassHierarchy.get(key);
    if (ret == null) {
      ret = new ClassHierarchyImpl(jars, parameterParsers);
      defaultClassHierarchy.put(key, ret);
    }
    return ret;
  }

  @Override
  public Injector newInjector(Configuration confs) {
    try {
      return newInjector(new Configuration[]{confs});
    } catch (BindException e) {
      throw new IllegalStateException("Unexpected error cloning configuration", e);
    }
  }

  @Override
  public Injector newInjector() {
    try {
      return newInjector(new Configuration[]{});
    } catch (BindException e) {
      throw new IllegalStateException("Unexpected error from empty configuration", e);
    }
  }

  private class SetValuedKey {
    public final Set<Object> key;

    public SetValuedKey(Object[] ts, Object[] us) {
      key = new HashSet<Object>(Arrays.asList(ts));
      key.addAll(Arrays.asList(us));
    }

    @Override
    public int hashCode() {
      int i = 0;
      for (Object t : key) {
        i += t.hashCode();
      }
      return i;
    }

    @Override
    public boolean equals(Object o) {
      SetValuedKey other = (SetValuedKey) o;
      if (other.key.size() != this.key.size()) {
        return false;
      }
      return key.containsAll(other.key);
    }
  }


}
