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
package com.microsoft.tang.implementation;

import com.microsoft.tang.*;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.ParseException;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.types.*;
import com.microsoft.tang.util.MonotonicMultiMap;
import com.microsoft.tang.util.TracingMonotonicMap;
import com.microsoft.tang.util.TracingMonotonicTreeMap;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  // TODO: Should be final again!
  public ClassHierarchy namespace;
  final TracingMonotonicMap<ClassNode<?>, ClassNode<?>> boundImpls = new TracingMonotonicTreeMap<>();
  final TracingMonotonicMap<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors = new TracingMonotonicTreeMap<>();
  final Map<NamedParameterNode<?>, String> namedParameters = new TracingMonotonicTreeMap<>();
  final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new TracingMonotonicTreeMap<>();
  final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> boundSetEntries = new MonotonicMultiMap<>();
  final TracingMonotonicMap<NamedParameterNode<List<?>>, List<Object>> boundLists = new TracingMonotonicTreeMap<>();

  public final static String IMPORT = "import";
  public final static String INIT = "<init>";

  protected ConfigurationBuilderImpl() {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  protected ConfigurationBuilderImpl(ClassHierarchy namespace) {
    this.namespace = namespace;
  }

  protected ConfigurationBuilderImpl(URL[] jars, Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parsers)
      throws BindException {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy(jars, parsers);
    for (Configuration tc : confs) {
      addConfiguration(((ConfigurationImpl) tc));
    }
  }

  protected ConfigurationBuilderImpl(ConfigurationBuilderImpl t) {
    this.namespace = t.getClassHierarchy();
    try {
      addConfiguration(t.getClassHierarchy(), t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(URL... jars) throws BindException {
    this(jars, new Configuration[0], new Class[0]);
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(Configuration... confs) throws BindException {
    this(new URL[0], confs, new Class[0]);
  }

  @Override
  public void addConfiguration(Configuration conf) throws BindException {
    // XXX remove cast!
    addConfiguration(conf.getClassHierarchy(), ((ConfigurationImpl) conf).builder);
  }

  @SuppressWarnings("unchecked")
  private <T> void addConfiguration(ClassHierarchy ns, ConfigurationBuilderImpl builder)
      throws BindException {
    namespace = namespace.merge(ns);
    if ((namespace instanceof ClassHierarchyImpl || builder.namespace instanceof ClassHierarchyImpl)) {
      if ((namespace instanceof ClassHierarchyImpl && builder.namespace instanceof ClassHierarchyImpl)) {
        ((ClassHierarchyImpl) namespace).parameterParser
            .mergeIn(((ClassHierarchyImpl) builder.namespace).parameterParser);
      } else {
        throw new IllegalArgumentException("Attempt to merge Java and non-Java class hierarchy!  Not supported.");
      }
    }

    for (ClassNode<?> cn : builder.boundImpls.keySet()) {
      bind(cn.getFullName(), builder.boundImpls.get(cn).getFullName());
    }
    for (ClassNode<?> cn : builder.boundConstructors.keySet()) {
      bind(cn.getFullName(), builder.boundConstructors.get(cn).getFullName());
    }
    // The namedParameters set contains the strings that can be used to
    // instantiate new
    // named parameter instances. Create new ones where we can.
    for (NamedParameterNode<?> np : builder.namedParameters.keySet()) {
      bind(np.getFullName(), builder.namedParameters.get(np));
    }
    for (ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn, builder.legacyConstructors.get(cn)
          .getArgs());
    }
    for (Entry<NamedParameterNode<Set<?>>, Object> e : builder.boundSetEntries) {
      String name = ((NamedParameterNode<Set<T>>) (NamedParameterNode<?>) e.getKey()).getFullName();
      if (e.getValue() instanceof Node) {
        bindSetEntry(name, (Node) e.getValue());
      } else if (e.getValue() instanceof String) {
        bindSetEntry(name, (String) e.getValue());
      } else {
        throw new IllegalStateException();
      }
    }
    // The boundLists set contains bound lists with their target NamedParameters
    for (NamedParameterNode<List<?>> np : builder.boundLists.keySet()) {
      bindList(np.getFullName(), builder.boundLists.get(np));
    }
  }

  @Override
  public ClassHierarchy getClassHierarchy() {
    return namespace;
  }

  @Override
  public void registerLegacyConstructor(ClassNode<?> c,
                                        final ConstructorArg... args) throws BindException {
    String cn[] = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      cn[i] = args[i].getType();
    }
    registerLegacyConstructor(c.getFullName(), cn);
  }

  @Override
  public void registerLegacyConstructor(String s, final String... args)
      throws BindException {
    ClassNode<?> cn = (ClassNode<?>) namespace.getNode(s);
    ClassNode<?>[] cnArgs = new ClassNode[args.length];
    for (int i = 0; i < args.length; i++) {
      cnArgs[i] = (ClassNode<?>) namespace.getNode(args[i]);
    }
    registerLegacyConstructor(cn, cnArgs);
  }

  @Override
  public void registerLegacyConstructor(ClassNode<?> cn,
                                        final ClassNode<?>... args) throws BindException {
    legacyConstructors.put(cn, cn.getConstructorDef(args));
  }

  @Override
  public <T> void bind(String key, String value) throws BindException {
    Node n = namespace.getNode(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Node m = namespace.getNode(value);
      bind((ClassNode<?>) n, (ClassNode<?>) m);
    } else {
      throw new IllegalStateException("getNode() returned " + n + " which is neither a ClassNode nor a NamedParameterNode");
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void bind(Node key, Node value) throws BindException {
    if (key instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) key, value.getFullName());
    } else if (key instanceof ClassNode) {
      ClassNode<?> k = (ClassNode<?>) key;
      if (value instanceof ClassNode) {
        ClassNode<?> val = (ClassNode<?>) value;
        if (val.isExternalConstructor() && !k.isExternalConstructor()) {
          bindConstructor(k, (ClassNode) val);
        } else {
          bindImplementation(k, (ClassNode) val);
        }
      }
    }
  }

  public <T> void bindImplementation(ClassNode<T> n, ClassNode<? extends T> m)
      throws BindException {
    if (namespace.isImplementation(n, m)) {
      boundImpls.put(n, m);
    } else {
      throw new IllegalArgumentException("Class" + m + " does not extend " + n);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> void bindParameter(NamedParameterNode<T> name, String value)
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
  public void bindSetEntry(String iface, String impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) namespace.getNode(iface), impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(String iface, Node impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) namespace.getNode(iface), impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, String impl)
      throws BindException {
    if (namespace instanceof ClassHierarchyImpl){
      JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
      try {
        javanamespace.parse(iface, impl);
      } catch (ParseException e) {
        throw new IllegalStateException("Could not parse " + impl + " which was passed to " + iface);
      }
    }
    boundSetEntries.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(NamedParameterNode<Set<T>> iface, Node impl)
      throws BindException {
    boundSetEntries.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindList(NamedParameterNode<List<T>> iface, List implList) {
    // Check parsability of list items
    for (Object item : implList) {
      if (item instanceof String) {
        JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
        try {
          // Just for parsability checking.
          javanamespace.parse(iface, (String) item);
        } catch(ParseException e) {
          throw new IllegalStateException("Could not parse " + item + " which was passed to " + iface);
        }
      }
    }
    boundLists.put((NamedParameterNode<List<?>>) (NamedParameterNode<?>) iface, implList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindList(String iface, List implList) {
    NamedParameterNode<List<?>> ifaceNode = (NamedParameterNode<List<?>>) namespace.getNode(iface);
    // Check parsability of list items
    for (Object item : implList) {
      if (item instanceof String) {
        JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
        try {
          // Just for parsability checking.
          javanamespace.parse(ifaceNode, (String) item);
        } catch(ParseException e) {
          throw new IllegalStateException("Could not parse " + item + " which was passed to " + iface);
        }
      }
    }
    boundLists.put(ifaceNode, implList);
  }

  @Override
  public <T> void bindConstructor(ClassNode<T> k,
                                  ClassNode<? extends ExternalConstructor<? extends T>> v) {
    boundConstructors.put(k, v);
  }

  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
  }

  @Override
  public String classPrettyDefaultString(String longName) throws NameResolutionException {
    final NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .getNode(longName);
    return param.getSimpleArgName() + "=" + join(",", param.getDefaultInstanceAsStrings());
  }

  private String join(String sep, String[] s) {
    if (s.length == 0) {
      return null;
    } else {
      StringBuilder sb = new StringBuilder(s[0]);
      for (int i = 1; i < s.length; i++) {
        sb.append(sep);
        sb.append(s[i]);
      }
      return sb.toString();
    }
  }

  @Override
  public String classPrettyDescriptionString(String fullName)
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

    ConfigurationBuilderImpl that = (ConfigurationBuilderImpl) o;

    if (boundConstructors != null ? !boundConstructors.equals(that.boundConstructors) : that.boundConstructors != null) {
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
    if (legacyConstructors != null ? !legacyConstructors.equals(that.legacyConstructors) : that.legacyConstructors != null) {
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
