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
import org.apache.reef.tang.implementation.types.NamedObjectElementImpl;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.MonotonicMultiMap;
import org.apache.reef.tang.util.TracingMonotonicMap;
import org.apache.reef.tang.util.TracingMonotonicTreeMap;

import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  public static final String IMPORT = "import";
  public static final String INIT = "<init>";
  protected final Map<NamedObjectElement, TracingMonotonicMap<ClassNode<?>, Boundable>> boundImpls =
      new TracingMonotonicTreeMap<>();
  protected final TracingMonotonicMap<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors =
      new TracingMonotonicTreeMap<>();
  protected final Map<NamedObjectElement, Map<NamedParameterNode<?>, Object>> namedParameters =
      new TracingMonotonicTreeMap<>();
  protected final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new TracingMonotonicTreeMap<>();
  protected final Map<NamedObjectElement, MonotonicMultiMap<NamedParameterNode<Set<?>>, Object>> boundSetEntries =
      new TracingMonotonicTreeMap();
  protected final Map<NamedObjectElement, TracingMonotonicMap<NamedParameterNode<List<?>>, List<Object>>> boundLists =
      new TracingMonotonicTreeMap<>();
  protected final NamedObjectElement nullNamedObjectElement = new NamedObjectElementImpl<>(null, null, null, true);
  // Set for managing namedObjectElements
  protected final Set<NamedObjectElement> namedObjectElements = new HashSet<>(Arrays.asList(nullNamedObjectElement));
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

    for (final NamedObjectElement<?> nno : builder.boundImpls.keySet()) {
      final TracingMonotonicMap<ClassNode<?>, Boundable> map = builder.boundImpls.get(nno);
      for (final Entry<ClassNode<?>, Boundable> entry : map.entrySet()) {
        bindImplementation(entry.getKey(), entry.getValue(), nno);
      }
    }
    for (final ClassNode<?> cn : builder.boundConstructors.keySet()) {
      bind(cn.getFullName(), builder.boundConstructors.get(cn).getFullName());
    }
    // The namedParameters set contains the strings that can be used to
    // instantiate new
    // named parameter instances. Create new ones where we can.
    for (final NamedObjectElement<?> nno : builder.namedParameters.keySet()) {
      final Map<NamedParameterNode<?>, Object> map = builder.namedParameters.get(nno);
      for (final Entry<NamedParameterNode<?>, Object> entry : map.entrySet()) {
        bindParameter(entry.getKey(), entry.getValue(), nno);
      }
    }
    for (final ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn, builder.legacyConstructors.get(cn)
          .getArgs());
    }
    for (final NamedObjectElement<?> nno : builder.boundSetEntries.keySet()) {
      final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> multiMap = builder.boundSetEntries.get(nno);
      for (final Entry<NamedParameterNode<Set<?>>, Object> e : multiMap) {
        final String name = ((NamedParameterNode<Set<T>>) (NamedParameterNode<?>) e.getKey()).getFullName();
        if (e.getValue() instanceof Node) {
          bindSetEntry(name, (Node) e.getValue(), nno);
        } else if (e.getValue() instanceof String) {
          bindSetEntry(name, (String) e.getValue(), nno);
        } else if (e.getValue() instanceof NamedObjectElement) {
          bindSetEntry(name, (NamedObjectElement) e.getValue(), nno);
        } else {
          throw new IllegalStateException("The value of the named parameter node in boundSetEntries"
              + " is neither String, Node nor NamedObjectElement. The actual type is  " + e.getValue().getClass());
        }
      }
    }
    // The boundLists set contains bound lists with their target NamedParameters
    for (final NamedObjectElement<?> nno : builder.boundLists.keySet()) {
      final TracingMonotonicMap<NamedParameterNode<List<?>>, List<Object>> map = builder.boundLists.get(nno);
      for (final Entry<NamedParameterNode<List<?>>, List<Object>> entry : map.entrySet()) {
        bindList(entry.getKey().getFullName(), entry.getValue());
      }
    }
  }

  private void addNamedObjectElement(final NamedObjectElement noe) throws BindException {
    if (noe.isNull()) {
      return;
    }
    // Check if there already exists another noe with same name
    for(final NamedObjectElement existingNoe: namedObjectElements) {
      if (existingNoe.getName().equals(noe.getName())
          && !existingNoe.getImplementationClass().equals(noe.getImplementationClass())) {
        throw new IllegalArgumentException("Attempt to configure two different NamedObjects with same name!");
      }
    }
    namedObjectElements.add(noe);
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
  public void bind(final String key, final String value, final NamedObjectElement namedObjectElement)
      throws BindException {
    final Node n = namespace.getNode(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value, namedObjectElement);
    } else if (n instanceof ClassNode) {
      final Node m = namespace.getNode(value);
      bind((ClassNode<?>) n, (ClassNode<?>) m, namedObjectElement);
    } else {
      throw new IllegalStateException("getNode() returned " + n +
          " which is neither a ClassNode nor a NamedParameterNode");
    }
  }

  @Override
  public void bind(final String key, final String value) throws BindException {
    bind(key, value, nullNamedObjectElement);
  }

  @Override
  public <T> void bind(final String key, final NamedObjectElement<T> impl, final NamedObjectElement namedObjectElement)
      throws BindException {
    final Node n = namespace.getNode(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, impl, namedObjectElement);
    } else if (n instanceof ClassNode) {
      bindImplementation((ClassNode<?>) n, impl, namedObjectElement);
    }
  }

  @Override
  public <T> void bind(final String key, final NamedObjectElement<T> impl) throws BindException {
    bind(key, impl, nullNamedObjectElement);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void bind(final Node key, final Node value, final NamedObjectElement namedObjectElement) throws BindException {
    if (key instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) key, value.getFullName(), namedObjectElement);
    } else if (key instanceof ClassNode) {
      final ClassNode<?> k = (ClassNode<?>) key;
      if (value instanceof ClassNode) {
        final ClassNode<?> val = (ClassNode<?>) value;
        if (val.isExternalConstructor() && !k.isExternalConstructor()) {
          bindConstructor(k, (ClassNode) val);
        } else {
          bindImplementation(k, (ClassNode) val, namedObjectElement);
        }
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void bind(final Node key, final Node value) throws BindException {
    bind(key, value, nullNamedObjectElement);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void bind(final Node key, final NamedObjectElement impl, final NamedObjectElement namedObjectElement)
      throws BindException {
    if (key instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) key, impl, namedObjectElement);
    } else if (key instanceof ClassNode) {
      bindImplementation((ClassNode<?>) key, impl, namedObjectElement);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void bind(final Node key, final NamedObjectElement impl) throws BindException {
    bind(key, impl, nullNamedObjectElement);
  }

  public <T> void bindImplementation(final ClassNode<T> n, final Object unknown,
                                     final NamedObjectElement namedObjectElement) throws BindException {
    if (unknown instanceof ClassNode) {
      final ClassNode<? extends T> m = (ClassNode) unknown;
      if (namespace.isImplementation(n, m)) {
        final TracingMonotonicMap<ClassNode<?>, Boundable> map;
        if (!boundImpls.containsKey(namedObjectElement)) {
          map = new TracingMonotonicTreeMap<>();
          boundImpls.put(namedObjectElement, map);
        } else {
          map = boundImpls.get(namedObjectElement);
        }
        map.put(n, m);
      } else {
        throw new IllegalArgumentException("Class" + m + " does not extend " + n);
      }
    } else if (unknown instanceof NamedObjectElement) {
      final NamedObjectElement<? extends T> impl = (NamedObjectElement) unknown;
      final ClassNode<? extends T> implType = impl.getTypeNode();
      if (namespace.isImplementation(n, implType)) {
        final TracingMonotonicMap<ClassNode<?>, Boundable> map;
        if (!boundImpls.containsKey(namedObjectElement)) {
          map = new TracingMonotonicTreeMap<>();
          boundImpls.put(namedObjectElement, map);
        } else {
          map = boundImpls.get(namedObjectElement);
        }
        map.put(n, impl);
        addNamedObjectElement(impl);
      } else {
        throw new IllegalArgumentException("Class" + implType + " does not extend " + n + "in NamedObject" +
            impl.getName());
      }
    } else {
      throw new IllegalArgumentException("Internal error: Tang tried to bind unsupported type to an interface");
    }
    if (!namedObjectElement.isNull()){
      addNamedObjectElement(namedObjectElement);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> void bindParameter(final NamedParameterNode name, final Object unknown,
                                final NamedObjectElement namedObjectElement)
      throws BindException {
    /* Parse and discard value; this is just for type checking */
    if (unknown instanceof String) {
      final String value = (String) unknown;
      if (namespace instanceof JavaClassHierarchy) {
        ((JavaClassHierarchy) namespace).parse(name, value);
      }
      if (name.isSet()) {
        bindSetEntry((NamedParameterNode) name, value, namedObjectElement);
      } else {
        final Map<NamedParameterNode<?>, Object> map;
        if (!namedParameters.containsKey(namedObjectElement)) {
          map = new TracingMonotonicTreeMap<>();
          namedParameters.put(namedObjectElement, map);
        } else {
          map = namedParameters.get(namedObjectElement);
        }
        map.put(name, value);
      }
    } else if (unknown instanceof NamedObjectElement) {
      final NamedObjectElement<? extends T> impl = (NamedObjectElement) unknown;
      if (name.isSet()) {
        bindSetEntry((NamedParameterNode<Set<T>>) name, impl, namedObjectElement);
      } else {
        Map<NamedParameterNode<?>, Object> map;
        if (!namedParameters.containsKey(namedObjectElement)) {
          map = new TracingMonotonicTreeMap<>();
          namedParameters.put(namedObjectElement, map);
        } else {
          map = namedParameters.get(namedObjectElement);
        }
        map.put(name, impl);
        addNamedObjectElement(impl);
      }
    } else {
      throw new BindException("Internal Error: Try to bind unknown type to a named parameter!");
    }
    if (!namedObjectElement.isNull()){
      addNamedObjectElement(namedObjectElement);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final String iface, final String impl, final NamedObjectElement namedObjectElement)
      throws BindException {
    bindSetEntry((NamedParameterNode<Set<T>>) namespace.getNode(iface), impl, namedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final String iface, final Node impl, final NamedObjectElement namedObjectElement)
      throws BindException {
    bindSetEntry((NamedParameterNode<Set<T>>) namespace.getNode(iface), impl, namedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final String iface, final NamedObjectElement impl,
                           final NamedObjectElement namedObjectElement)
      throws BindException {
    bindSetEntry((NamedParameterNode<Set<T>>) namespace.getNode(iface), impl, namedObjectElement);
  }
  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final String impl,
                               final NamedObjectElement namedObjectElement)
      throws BindException {
    if (namespace instanceof ClassHierarchyImpl) {
      final JavaClassHierarchy javanamespace = (ClassHierarchyImpl) namespace;
      try {
        javanamespace.parse(iface, impl);
      } catch (final ParseException e) {
        throw new IllegalStateException("Could not parse " + impl + " which was passed to " + iface, e);
      }
    }
    final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> map;
    if (!boundSetEntries.containsKey(namedObjectElement)) {
      map = new MonotonicMultiMap<>();
      boundSetEntries.put(namedObjectElement, map);
    } else {
      map = boundSetEntries.get(namedObjectElement);
    }
    map.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final Node impl,
                               final NamedObjectElement namedObjectElement)
      throws BindException {
    final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> map;
    if (!boundSetEntries.containsKey(namedObjectElement)) {
      map = new MonotonicMultiMap<>();
      boundSetEntries.put(namedObjectElement, map);
    } else {
      map = boundSetEntries.get(namedObjectElement);
    }
    map.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
  }

  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface,
                               final NamedObjectElement<? extends T> impl, final NamedObjectElement namedObjectElement)
      throws BindException {
    final MonotonicMultiMap<NamedParameterNode<Set<?>>, Object> map;
    if (!boundSetEntries.containsKey(namedObjectElement)) {
      map = new MonotonicMultiMap<>();
      boundSetEntries.put(namedObjectElement, map);
    } else {
      map = boundSetEntries.get(namedObjectElement);
    }
    map.put((NamedParameterNode<Set<?>>) (NamedParameterNode<?>) iface, impl);
    addNamedObjectElement(impl);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(final String iface, final String impl) throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(final String iface, final Node impl) throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindSetEntry(final String iface, final NamedObjectElement impl) throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final String impl) throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final Node impl) throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(final NamedParameterNode<Set<T>> iface, final NamedObjectElement<? extends T> impl)
      throws BindException {
    bindSetEntry(iface, impl, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindList(final NamedParameterNode<List<T>> iface, final List implList,
                           final NamedObjectElement namedObjectElement) throws BindException {
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
      } else if (item instanceof NamedObjectElement) {
        addNamedObjectElement((NamedObjectElement) item);
      }
    }
    final TracingMonotonicMap<NamedParameterNode<List<?>>, List<Object>> map;
    if (!boundLists.containsKey(namedObjectElement)) {
      map = new TracingMonotonicTreeMap<>();
      boundLists.put(namedObjectElement, map);
    } else {
      map = boundLists.get(namedObjectElement);
    }
    map.put((NamedParameterNode<List<?>>) (NamedParameterNode<?>) iface, implList);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindList(final String iface, final List implList, final NamedObjectElement namedObjectElement)
      throws BindException {
    // Check parsability of list items
    bindList((NamedParameterNode<List<T>>) namespace.getNode(iface), implList, namedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindList(final NamedParameterNode<List<T>> iface, final List implList) throws BindException {
    bindList(iface, implList, nullNamedObjectElement);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bindList(final String iface, final List implList) throws BindException {
    bindList(iface, implList, nullNamedObjectElement);
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
