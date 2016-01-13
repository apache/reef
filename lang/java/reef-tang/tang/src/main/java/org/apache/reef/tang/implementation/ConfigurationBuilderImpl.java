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
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.exceptions.ParseException;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.MonotonicMultiMap;
import org.apache.reef.tang.util.TracingMonotonicMap;
import org.apache.reef.tang.util.TracingMonotonicTreeMap;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  public static final String IMPORT = "import";
  public static final String INIT = "<init>";
  protected final TracingMonotonicMap<ClassNode<?>, ClassNode<?>> boundImpls = new TracingMonotonicTreeMap<>();
  protected final TracingMonotonicMap<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors =
      new TracingMonotonicTreeMap<>();
  protected final Map<NamedParameterNode<?>, String> namedParameters = new TracingMonotonicTreeMap<>();
  protected final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new TracingMonotonicTreeMap<>();
  protected final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> boundSetEntries = new MonotonicMultiMap<>();
  protected final TracingMonotonicMap<NamedParameterNode<List<?>>, List<Object>> boundLists =
      new TracingMonotonicTreeMap<>();
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  // TODO: Should be final again!
  private ClassHierarchy namespace;

  protected ConfigurationBuilderImpl() {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  protected ConfigurationBuilderImpl(final ClassHierarchy namespace) {
    this.namespace = namespace;
  }

  protected ConfigurationBuilderImpl(final URL[] jars,
                                     final Configuration[] confs,
                                     final Class<? extends ExternalConstructor<?>>[] parsers)
      throws BindException {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy(jars, parsers);
    for (final Configuration tc : confs) {
      addConfiguration((ConfigurationImpl) tc);
    }
  }

  protected ConfigurationBuilderImpl(final ConfigurationBuilderImpl t) {
    this.namespace = t.getClassHierarchy();
    try {
      addConfiguration(t.getClassHierarchy(), t);
    } catch (final BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(final URL... jars) throws BindException {
    this(jars, new Configuration[0], new Class[0]);
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(final Configuration... confs) throws BindException {
    this(new URL[0], confs, new Class[0]);
  }

  @Override
  public void addConfiguration(final Configuration conf) throws BindException {
    // XXX remove cast!
    addConfiguration(conf.getClassHierarchy(), ((ConfigurationImpl) conf).getBuilder());
  }

  @SuppressWarnings("unchecked")
  private <T> void addConfiguration(final ClassHierarchy ns, final ConfigurationBuilderImpl builder)
      throws BindException {
    namespace = namespace.merge(ns);
    if (namespace instanceof ClassHierarchyImpl || builder.namespace instanceof ClassHierarchyImpl) {
      if (namespace instanceof ClassHierarchyImpl && builder.namespace instanceof ClassHierarchyImpl) {
        ((ClassHierarchyImpl) namespace).getParameterParser()
            .mergeIn(((ClassHierarchyImpl) builder.namespace).getParameterParser());
      } else {
        throw new IllegalArgumentException("Attempt to merge Java and non-Java class hierarchy!  Not supported.");
      }
    }

    for (final ClassNode<?> cn : builder.boundImpls.keySet()) {
      bind(cn.getFullName(), builder.boundImpls.get(cn).getFullName());
    }
    for (final ClassNode<?> cn : builder.boundConstructors.keySet()) {
      bind(cn.getFullName(), builder.boundConstructors.get(cn).getFullName());
    }
    // The namedParameters set contains the strings that can be used to
    // instantiate new
    // named parameter instances. Create new ones where we can.
    for (final NamedParameterNode<?> np : builder.namedParameters.keySet()) {
      bind(np.getFullName(), builder.namedParameters.get(np));
    }
    for (final ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn, builder.legacyConstructors.get(cn)
          .getArgs());
    }
    for (final Entry<NamedParameterNode<Set<?>>, Object> e : builder.boundSetEntries) {
      final String name = ((NamedParameterNode<Set<T>>) (NamedParameterNode<?>) e.getKey()).getFullName();
      if (e.getValue() instanceof Node) {
        bindSetEntry(name, (Node) e.getValue());
      } else if (e.getValue() instanceof String) {
        bindSetEntry(name, (String) e.getValue());
      } else {
        throw new IllegalStateException();
      }
    }
    // The boundLists set contains bound lists with their target NamedParameters
    for (final NamedParameterNode<List<?>> np : builder.boundLists.keySet()) {
      bindList(np.getFullName(), builder.boundLists.get(np));
    }
  }

  @Override
  public ClassHierarchy getClassHierarchy() {
    return namespace;
  }

  @Override
  public void registerLegacyConstructor(final ClassNode<?> c,
                                        final ConstructorArg... args) throws BindException {
    final String[] cn = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      cn[i] = args[i].getType();
    }
    registerLegacyConstructor(c.getFullName(), cn);
  }

  @Override
  public void registerLegacyConstructor(final String s, final String... args)
      throws BindException {
    final ClassNode<?> cn = (ClassNode<?>) namespace.getNode(s);
    final ClassNode<?>[] cnArgs = new ClassNode[args.length];
    for (int i = 0; i < args.length; i++) {
      cnArgs[i] = (ClassNode<?>) namespace.getNode(args[i]);
    }
    registerLegacyConstructor(cn, cnArgs);
  }

  @Override
  public void registerLegacyConstructor(final ClassNode<?> cn,
                                        final ClassNode<?>... args) throws BindException {
    legacyConstructors.put(cn, cn.getConstructorDef(args));
  }

  @Override
  public <T> void bind(final String key, final String value) throws BindException {
    final Node n = namespace.getNode(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      final Node m = namespace.getNode(value);
      bind((ClassNode<?>) n, (ClassNode<?>) m);
    } else {
      throw new IllegalStateException("getNode() returned " + n +
          " which is neither a ClassNode nor a NamedParameterNode");
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void bind(final Node key, final Node value) throws BindException {
    if (key instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) key, value.getFullName());
    } else if (key instanceof ClassNode) {
      final ClassNode<?> k = (ClassNode<?>) key;
      if (value instanceof ClassNode) {
        final ClassNode<?> val = (ClassNode<?>) value;
        if (val.isExternalConstructor() && !k.isExternalConstructor()) {
          bindConstructor(k, (ClassNode) val);
        } else {
          bindImplementation(k, (ClassNode) val);
        }
      }
    }
  }

  public <T> void bindImplementation(final ClassNode<T> n, final ClassNode<? extends T> m)
      throws BindException {
    if (namespace.isImplementation(n, m)) {
      boundImpls.put(n, m);
    } else {
      throw new IllegalArgumentException("Class" + m + " does not extend " + n);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> void bindParameter(final NamedParameterNode<T> name, final String value)
      throws BindException {
    /* Parse and discard value; this is just for type checking */
    if (namespace instanceof JavaClassHierarchy) {
      ((JavaClassHierarchy) namespace).parse(name, value);
    }
    if (name.isSet()) {
      bindSetEntry((NamedParameterNode) name, value);
    } else {
      namedParameters.put(name, value);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(final String iface, final String impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) namespace.getNode(iface), impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(final String iface, final Node impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) namespace.getNode(iface), impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final String impl)
      throws BindException {
    if (namespace instanceof ClassHierarchyImpl) {
      final JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
      try {
        javanamespace.parse(iface, impl);
      } catch (final ParseException e) {
        throw new IllegalStateException("Could not parse " + impl + " which was passed to " + iface, e);
      }
    }
    boundSetEntries.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final Node impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindList(final NamedParameterNode<List<T>> iface, final List implList) {
    // Check parsability of list items
    for (final Object item : implList) {
      if (item instanceof String) {
        final JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
        try {
          // Just for parsability checking.
          javanamespace.parse(iface, (String) item);
        } catch (final ParseException e) {
          throw new IllegalStateException("Could not parse " + item + " which was passed to " + iface, e);
        }
      }
    }
    boundLists.put((NamedParameterNode<List<?>>) (NamedParameterNode<?>) iface, implList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindList(final String iface, final List implList) {
    final NamedParameterNode<List<?>> ifaceNode = (NamedParameterNode<List<?>>) namespace.getNode(iface);
    // Check parsability of list items
    for (final Object item : implList) {
      if (item instanceof String) {
        final JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
        try {
          // Just for parsability checking.
          javanamespace.parse(ifaceNode, (String) item);
        } catch (final ParseException e) {
          throw new IllegalStateException("Could not parse " + item + " which was passed to " + iface, e);
        }
      }
    }
    boundLists.put(ifaceNode, implList);
  }

  @Override
  public <T> void bindConstructor(final ClassNode<T> k,
                                  final ClassNode<? extends ExternalConstructor<? extends T>> v) {
    boundConstructors.put(k, v);
  }

  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
  }

  @Override
  public String classPrettyDefaultString(final String longName) throws NameResolutionException {
    final NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .getNode(longName);
    return param.getSimpleArgName() + "=" + join(",", param.getDefaultInstanceAsStrings());
  }

  private String join(final String sep, final String[] s) {
    if (s.length == 0) {
      return null;
    } else {
      final StringBuilder sb = new StringBuilder(s[0]);
      for (int i = 1; i < s.length; i++) {
        sb.append(sep);
        sb.append(s[i]);
      }
      return sb.toString();
    }
  }

  @Override
  public String classPrettyDescriptionString(final String fullName)
      throws NameResolutionException {
    final NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .getNode(fullName);
    return param.getDocumentation() + "\n" + param.getFullName();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ConfigurationBuilderImpl that = (ConfigurationBuilderImpl) o;

    if (boundConstructors != null ?
        !boundConstructors.equals(that.boundConstructors) : that.boundConstructors != null) {
      return false;
    }
    if (boundImpls != null ? !boundImpls.equals(that.boundImpls) : that.boundImpls != null) {
      return false;
    }
    if (boundSetEntries != null ? !boundSetEntries.equals(that.boundSetEntries) : that.boundSetEntries != null) {
      return false;
    }
    if (boundLists != null ? !boundLists.equals(that.boundLists) : that.boundLists != null) {
      return false;
    }
    if (legacyConstructors != null ?
        !legacyConstructors.equals(that.legacyConstructors) : that.legacyConstructors != null) {
      return false;
    }
    if (namedParameters != null ? !namedParameters.equals(that.namedParameters) : that.namedParameters != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = boundImpls != null ? boundImpls.hashCode() : 0;
    result = 31 * result + (boundConstructors != null ? boundConstructors.hashCode() : 0);
    result = 31 * result + (namedParameters != null ? namedParameters.hashCode() : 0);
    result = 31 * result + (legacyConstructors != null ? legacyConstructors.hashCode() : 0);
    result = 31 * result + (boundSetEntries != null ? boundSetEntries.hashCode() : 0);
    result = 31 * result + (boundLists != null ? boundLists.hashCode() : 0);
    return result;
  }
}
