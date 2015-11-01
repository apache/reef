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
package org.apache.reef.tang.formats;

import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.util.MonotonicHashMap;
import org.apache.reef.tang.util.MonotonicHashSet;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConfigurationModuleBuilder {

  private static final Set<Class<?>> PARAM_BLACKLIST = new MonotonicHashSet<>(
      Param.class, Impl.class);
  private static final Set<Class<?>> PARAM_TYPES = new MonotonicHashSet<>(
      RequiredImpl.class, OptionalImpl.class, RequiredParameter.class,
      OptionalParameter.class);
  protected final JavaConfigurationBuilder b = Tang.Factory.getTang()
      .newConfigurationBuilder();
  // Sets of things that have been declared
  protected final Set<Field> reqDecl = new MonotonicHashSet<>();
  protected final Set<Object> setOpts = new MonotonicHashSet<>();
  // Maps from field instance variables to the fields that they
  // are assigned to. These better be unique!
  protected Map<Object, Field> map = new MonotonicHashMap<>();
  protected final Map<Class<?>, Impl<?>> freeImpls = new MonotonicHashMap<>();
  protected final Map<Class<? extends Name<?>>, Param<?>> freeParams = new MonotonicHashMap<>();
  private final Set<Field> optDecl = new MonotonicHashSet<>();
  // Set of things that have been used in a bind. These must be equal
  // to the decl counterparts before build() is called.
  private final Set<Field> reqUsed = new MonotonicHashSet<>();
  private final Set<Field> optUsed = new MonotonicHashSet<>();
  private final Map<Class<?>, String> lateBindClazz = new MonotonicHashMap<>();

  protected ConfigurationModuleBuilder() {
    for (final Field f : getClass().getDeclaredFields()) {
      final Class<?> t = f.getType();
      if (PARAM_BLACKLIST.contains(t)) {
        throw new ClassHierarchyException(
            "Found a field of type " + t + " which should be a Required/Optional Parameter/Implementation instead"
        );
      }
      if (PARAM_TYPES.contains(t)) {
        if (!Modifier.isPublic(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-public configuration option in " + getClass() + ": "
                  + f);
        }
        if (!Modifier.isStatic(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-static configuration option in " + getClass() + ": "
                  + f);
        }
        if (!Modifier.isFinal(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-final configuration option in " + getClass() + ": "
                  + f);
        }
        final Object o;
        try {
          o = f.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new ClassHierarchyException(
              "Could not look up field instance in " + getClass() + " field: "
                  + f, e);
        }
        if (map.containsKey(o)) {
          throw new ClassHierarchyException(
              "Detected aliased instances in class " + getClass()
                  + " for fields " + map.get(o) + " and " + f);
        }
        if (t == RequiredImpl.class || t == RequiredParameter.class) {
          reqDecl.add(f);
        } else {
          optDecl.add(f);
        }
        map.put(o, f);
      }
    }
  }

  private ConfigurationModuleBuilder(final ConfigurationModuleBuilder c) {
    try {
      b.addConfiguration(c.b.build());
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    reqDecl.addAll(c.reqDecl);
    optDecl.addAll(c.optDecl);
    reqUsed.addAll(c.reqUsed);
    optUsed.addAll(c.optUsed);
    setOpts.addAll(c.setOpts);
    map.putAll(c.map);
    freeImpls.putAll(c.freeImpls);
    freeParams.putAll(c.freeParams);
    lateBindClazz.putAll(c.lateBindClazz);

  }

  /**
   * TODO: It would be nice if this incorporated d by reference so that static analysis / documentation tools
   * could document the dependency between c and d.
   *
   * @param d a configuration module to merge
   * @return the merged configuration module builder
   */
  public final ConfigurationModuleBuilder merge(final ConfigurationModule d) {
    if (d == null) {
      throw new NullPointerException("If merge() was passed a static final field that is initialized to non-null, " +
          "then this is almost certainly caused by a circular class dependency.");
    }
    try {
      d.assertStaticClean();
    } catch (final ClassHierarchyException e) {
      throw new ClassHierarchyException(ReflectionUtilities.getFullName(getClass()) +
          ": detected attempt to merge with ConfigurationModule that has had set() called on it", e);
    }
    final ConfigurationModuleBuilder c = deepCopy();
    final ConfigurationModuleBuilder builder = d.getBuilder();
    try {
      c.b.addConfiguration(builder.b.build());
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    c.reqDecl.addAll(builder.reqDecl);
    c.optDecl.addAll(builder.optDecl);
    c.reqUsed.addAll(builder.reqUsed);
    c.optUsed.addAll(builder.optUsed);
    c.setOpts.addAll(builder.setOpts);
    c.map.putAll(builder.map);
    c.freeImpls.putAll(builder.freeImpls);
    c.freeParams.putAll(builder.freeParams);
    c.lateBindClazz.putAll(builder.lateBindClazz);

    return c;
  }

  public final <T> ConfigurationModuleBuilder bind(final Class<?> iface, final Impl<?> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindSetEntry(
      final Class<? extends Name<Set<T>>> iface, final String impl) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindSetEntry(iface, impl);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                           final Class<? extends T> impl) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindSetEntry(iface, impl);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                           final Impl<? extends T> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    if (!setOpts.contains(opt)) {
      c.setOpts.add(opt);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindSetEntry(final Class<? extends Name<Set<T>>> iface,
                                                           final Param<? extends T> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(iface, opt);
    if (!setOpts.contains(opt)) {
      c.setOpts.add(opt);
    }
    return c;
  }


  public final <T> ConfigurationModuleBuilder bindImplementation(final Class<T> iface,
                                                                 final Class<? extends T> impl) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindImplementation(iface, impl);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindImplementation(final Class<T> iface,
                                                                 final String impl) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.lateBindClazz.put(iface, impl);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindImplementation(final Class<T> iface,
                                                                 final Impl<? extends T> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      final Class<? extends Name<T>> name, final String value) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindNamedParameter(name, value);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      final Class<? extends Name<T>> name, final Param<T> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(name, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      final Class<? extends Name<T>> iface, final Class<? extends T> impl) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindNamedParameter(iface, impl);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      final Class<? extends Name<T>> iface, final Impl<? extends T> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindConstructor(final Class<T> clazz,
                                                              final Class<? extends ExternalConstructor<? extends T>>
                                                              constructor) {
    final ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindConstructor(clazz, constructor);
    } catch (final BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindConstructor(
      final Class<T> cons, final Impl<? extends ExternalConstructor<? extends T>> v) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(v);
    c.freeImpls.put(cons, v);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindList(final Class<? extends Name<List<T>>> iface,
                                                       final Impl<List> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindList(final Class<? extends Name<List<T>>> iface,
                                                       final Param<List> opt) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindList(final Class<? extends Name<List<T>>> iface, final List list) {
    final ConfigurationModuleBuilder c = deepCopy();
    c.b.bindList(iface, list);
    return c;
  }

  private <T> void processUse(final Object impl) {
    final Field f = map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when binding " +
          ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
    }
    if (!reqUsed.contains(f)) {
      reqUsed.add(f);
    }
    if (!optUsed.contains(f)) {
      optUsed.add(f);
    }
  }

  public final ConfigurationModule build() throws ClassHierarchyException {
    final ConfigurationModuleBuilder c = deepCopy();

    if (!(c.reqUsed.containsAll(c.reqDecl) && c.optUsed.containsAll(c.optDecl))) {
      final Set<Field> fset = new MonotonicHashSet<>();
      for (final Field f : c.reqDecl) {
        if (!c.reqUsed.contains(f)) {
          fset.add(f);
        }
      }
      for (final Field f : c.optDecl) {
        if (!c.optUsed.contains(f)) {
          fset.add(f);
        }
      }
      throw new ClassHierarchyException(
          "Found declared options that were not used in binds: "
              + toString(fset));
    }
    for (final Class<?> clz : c.lateBindClazz.keySet()) {
      try {
        c.b.bind(ReflectionUtilities.getFullName(clz), c.lateBindClazz.get(clz));
      } catch (final NameResolutionException e) {
        throw new ClassHierarchyException("ConfigurationModule refers to unknown class: " +
            c.lateBindClazz.get(clz), e);
      } catch (final BindException e) {
        throw new ClassHierarchyException("bind failed while initializing ConfigurationModuleBuilder", e);
      }
    }
    return new ConfigurationModule(c);
  }

/*  public final <T> ConfigurationModuleBuilder bind(Class<T> iface, Class<?> impl) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bind(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  } */

  final ConfigurationModuleBuilder deepCopy() {
    // ooh... this is a dirty trick --- we strip this's type off here,
    // fortunately, we've all ready looked at the root object's class's
    // fields, and we copy the information we extracted from them, so
    // everything works out OK w.r.t. field detection.
    return new ConfigurationModuleBuilder(this) {
    };
  }

  final String toString(final Set<Field> s) {
    final StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (final Field f : s) {
      sb.append((first ? " " : ", ") + f.getName());
      first = false;
    }
    sb.append(" }");
    return sb.toString();
  }
}
