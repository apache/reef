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

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.JavaClassHierarchy;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.exceptions.ParseException;
import org.apache.reef.tang.formats.ParameterParser;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.MonotonicTreeMap;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class ClassHierarchyImpl implements JavaClassHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.  Tint works around this using such
  // a library.

  /**
   * The ParameterParser that this ClassHierarchy uses to parse default values.
   * Custom parameter parsers allow applications to extend the set of classes
   * that Tang can parse.
   */
  private final ParameterParser parameterParser = new ParameterParser();
  /**
   * The classloader that was used to populate this class hierarchy.
   */
  private final URLClassLoader loader;
  /**
   * The jars that are reflected by that loader.  These are in addition to
   * whatever jars are available by the default classloader (i.e., the one
   * that loaded Tang.  We need this list so that we can merge class hierarchies
   * that are backed by different classpaths.
   */
  private final List<URL> jars;
  /**
   * A reference to the root package which is a root of a tree of nodes.
   * The children of each node in the tree are accessible by name, and the
   * structure of the tree mirrors Java's package namespace.
   */
  private final PackageNode namespace;
  /**
   * A map from short name to named parameter node.  This is only used to
   * sanity check short names so that name clashes get resolved.
   */
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicTreeMap<>();

  @SuppressWarnings("unchecked")
  public ClassHierarchyImpl() {
    this(new URL[0], new Class[0]);
  }

  @SuppressWarnings("unchecked")
  public ClassHierarchyImpl(final URL... jars) {
    this(jars, new Class[0]);
  }

  public ClassHierarchyImpl(final URL[] jars, final Class<? extends ExternalConstructor<?>>[] parameterParsers) {
    this.namespace = JavaNodeFactory.createRootPackageNode();
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
    for (final Class<? extends ExternalConstructor<?>> p : parameterParsers) {
      try {
        parameterParser.addParser(p);
      } catch (final BindException e) {
        throw new IllegalArgumentException("Could not register parameter parsers", e);
      }
    }
  }

  /**
   * A helper method that returns the parsed default value of a given
   * NamedParameter.
   *
   * @return null or an empty set if there is no default value, the default value (or set of values) otherwise.
   * @throws ClassHierarchyException if a default value was specified, but could not be parsed, or if a set of
   *                                 values were specified for a non-set parameter.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T parseDefaultValue(final NamedParameterNode<T> name) {
    final String[] vals = name.getDefaultInstanceAsStrings();
    final T[] ret = (T[]) new Object[vals.length];
    for (int i = 0; i < vals.length; i++) {
      final String val = vals[i];
      try {
        ret[i] = parse(name, val);
      } catch (final ParseException e) {
        throw new ClassHierarchyException("Could not parse default value", e);
      }
    }
    if (name.isSet()) {
      return (T) new HashSet<T>(Arrays.asList(ret));
    } else if (name.isList()) {
      return (T) new ArrayList<T>(Arrays.asList(ret));
    } else {
      if (ret.length == 0) {
        return null;
      } else if (ret.length == 1) {
        return ret[0];
      } else {
        throw new IllegalStateException("Multiple defaults for non-set named parameter! " + name.getFullName());
      }
    }
  }

  /**
   * Parse a string, assuming that it is of the type expected by a given NamedParameter.
   * <p>
   * This method does not deal with sets; if the NamedParameter is set valued, then the provided
   * string should correspond to a single member of the set.  It is up to the caller to call parse
   * once for each value that should be parsed as a member of the set.
   *
   * @return a non-null reference to the parsed value.
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T parse(final NamedParameterNode<T> np, final String value) throws ParseException {
    final ClassNode<T> iface;
    try {
      iface = (ClassNode<T>) getNode(np.getFullArgName());
    } catch (final NameResolutionException e) {
      throw new IllegalStateException("Could not parse validated named parameter argument type.  NamedParameter is " +
          np.getFullName() + " argument type is " + np.getFullArgName(), e);
    }
    Class<?> clazz;
    String fullName;
    try {
      clazz = classForName(iface.getFullName());
      fullName = null;
    } catch (final ClassNotFoundException e) {
      clazz = null;
      fullName = iface.getFullName();
    }
    try {
      if (clazz != null) {
        return (T) parameterParser.parse(clazz, value);
      } else {
        return parameterParser.parse(fullName, value);
      }
    } catch (final UnsupportedOperationException e) {
      try {
        final Node impl = getNode(value);
        if (impl instanceof ClassNode && isImplementation(iface, (ClassNode<?>) impl)) {
          return (T) impl;
        }
        throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() +
            " cannot take non-subclass " + impl.getFullName(), e);
      } catch (final NameResolutionException e2) {
        throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() +
            " cannot take non-class " + value, e2);
      }
    }
  }

  /**
   * Helper method that converts a String to a Class using this
   * ClassHierarchy's classloader.
   */
  @Override
  public Class<?> classForName(final String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }

  private <T, U> Node buildPathToNode(final Class<U> clazz)
      throws ClassHierarchyException {
    final String[] path = clazz.getName().split("\\$");

    Node root = namespace;
    for (int i = 0; i < path.length - 1; i++) {
      root = root.get(path[i]);
    }

    if (root == null) {
      throw new NullPointerException();
    }
    final Node parent = root;

    final Type argType = ReflectionUtilities.getNamedParameterTargetOrNull(clazz);

    if (argType == null) {
      return JavaNodeFactory.createClassNode(parent, clazz);
    } else {

      // checked inside of NamedParameterNode, using reflection.
      @SuppressWarnings("unchecked") final NamedParameterNode<T> np = JavaNodeFactory.createNamedParameterNode(
          parent, (Class<? extends Name<T>>) clazz, argType);

      if (parameterParser.canParse(ReflectionUtilities.getFullName(argType)) &&
          clazz.getAnnotation(NamedParameter.class).default_class() != Void.class) {
        throw new ClassHierarchyException("Named parameter " + ReflectionUtilities.getFullName(clazz) +
            " defines default implementation for parsable type " + ReflectionUtilities.getFullName(argType));
      }

      final String shortName = np.getShortName();
      if (shortName != null) {
        final NamedParameterNode<?> oldNode = shortNames.get(shortName);
        if (oldNode != null) {
          if (oldNode.getFullName().equals(np.getFullName())) {
            throw new IllegalStateException("Tried to double bind "
                + oldNode.getFullName() + " to short name " + shortName);
          }
          throw new ClassHierarchyException("Named parameters " + oldNode.getFullName()
              + " and " + np.getFullName() + " have the same short name: "
              + shortName);
        }
        shortNames.put(shortName, np);
      }
      return np;
    }
  }

  private Node getAlreadyBoundNode(final Class<?> clazz) throws NameResolutionException {
    return getAlreadyBoundNode(ReflectionUtilities.getFullName(clazz));
  }

  @Override
  public Node getNode(final Class<?> clazz) {
    try {
      return getNode(ReflectionUtilities.getFullName(clazz));
    } catch (final NameResolutionException e) {
      throw new ClassHierarchyException("JavaClassHierarchy could not resolve " + clazz
          + " which is definitely avalable at runtime", e);
    }
  }

  @Override
  public synchronized Node getNode(final String name) throws NameResolutionException {
    final Node n = register(name);
    if (n == null) {
      // This will never succeed; it just generates a nice exception.
      getAlreadyBoundNode(name);
      throw new IllegalStateException("IMPLEMENTATION BUG: Register failed, "
          + "but getAlreadyBoundNode succeeded!");
    }
    return n;
  }

  private Node getAlreadyBoundNode(final String name) throws NameResolutionException {
    Node root = namespace;
    final String[] toks = name.split("\\$");
    final String outerClassName = toks[0];
    root = root.get(outerClassName);
    if (root == null) {
      throw new NameResolutionException(name, outerClassName);
    }
    for (int i = 1; i < toks.length; i++) {
      root = root.get(toks[i]);
      if (root == null) {
        final StringBuilder sb = new StringBuilder(outerClassName);
        for (int j = 0; j < i; j++) {
          sb.append(toks[j]);
          if (j != i - 1) {
            sb.append(".");
          }
        }
        throw new NameResolutionException(name, sb.toString());
      }
    }
    return root;
  }

  private Node register(final String s) {
    final Class<?> c;
    try {
      c = classForName(s);
    } catch (final ClassNotFoundException e1) {
      return null;
    }
    try {
      final Node n = getAlreadyBoundNode(c);
      return n;
    } catch (final NameResolutionException ignored) {
      // node not bound yet
    }
    // First, walk up the class hierarchy, registering all out parents. This
    // can't be loopy.
    if (c.getSuperclass() != null) {
      register(ReflectionUtilities.getFullName(c.getSuperclass()));
    }
    for (final Class<?> i : c.getInterfaces()) {
      register(ReflectionUtilities.getFullName(i));
    }
    // Now, we'd like to register our enclosing classes. This turns out to be
    // safe.
    // Thankfully, Java doesn't allow:
    // class A implements A.B { class B { } }

    // It also doesn't allow cycles such as:
    // class A implements B.BB { interface AA { } }
    // class B implements A.AA { interface BB { } }

    // So, even though grafting arbitrary DAGs together can give us cycles, Java
    // seems
    // to have our back on this one.
    final Class<?> enclosing = c.getEnclosingClass();
    if (enclosing != null) {
      register(ReflectionUtilities.getFullName(enclosing));
    }

    // Now register the class. This has to be after the above so we know our
    // parents (superclasses and enclosing packages) are already registered.
    final Node n = registerClass(c);

    // Finally, do things that might introduce cycles that invlove c.
    // This has to be below registerClass, which ensures that any cycles
    // this stuff introduces are broken.
    for (final Class<?> innerClass : c.getDeclaredClasses()) {
      register(ReflectionUtilities.getFullName(innerClass));
    }
    if (n instanceof ClassNode) {
      final ClassNode<?> cls = (ClassNode<?>) n;
      for (final ConstructorDef<?> def : cls.getInjectableConstructors()) {
        for (final ConstructorArg arg : def.getArgs()) {
          register(arg.getType());
          if (arg.getNamedParameterName() != null) {
            final NamedParameterNode<?> np = (NamedParameterNode<?>) register(arg
                .getNamedParameterName());
            try {
              // TODO: When handling sets, need to track target of generic parameter, and check the type here!
              if (!np.isSet() && !np.isList() &&
                  !ReflectionUtilities.isCoercable(classForName(arg.getType()), classForName(np.getFullArgName()))) {
                throw new ClassHierarchyException(
                    "Named parameter type mismatch in " + cls.getFullName() + ".  Constructor expects a "
                        + arg.getType() + " but " + np.getName() + " is a "
                        + np.getFullArgName());
              }
            } catch (final ClassNotFoundException e) {
              throw new ClassHierarchyException("Constructor refers to unknown class "
                  + arg.getType(), e);
            }
          }
        }
      }
    } else if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      register(np.getFullArgName());
    }
    return n;
  }

  /**
   * Assumes that all of the parents of c have been registered already.
   *
   * @param c
   */
  @SuppressWarnings("unchecked")
  private <T> Node registerClass(final Class<T> c)
      throws ClassHierarchyException {
    if (c.isArray()) {
      throw new UnsupportedOperationException("Can't register array types");
    }
    try {
      return getAlreadyBoundNode(c);
    } catch (final NameResolutionException ignored) {
      // node not bound yet
    }

    final Node n = buildPathToNode(c);

    if (n instanceof ClassNode) {
      final ClassNode<T> cn = (ClassNode<T>) n;
      final Class<T> superclass = (Class<T>) c.getSuperclass();
      if (superclass != null) {
        try {
          ((ClassNode<T>) getAlreadyBoundNode(superclass)).putImpl(cn);
        } catch (final NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
      for (final Class<?> interf : c.getInterfaces()) {
        try {
          ((ClassNode<T>) getAlreadyBoundNode(interf)).putImpl(cn);
        } catch (final NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return n;
  }

  @Override
  public PackageNode getNamespace() {
    return namespace;
  }

  public ParameterParser getParameterParser() {
    return parameterParser;
  }

  @Override
  public synchronized boolean isImplementation(final ClassNode<?> inter, final ClassNode<?> impl) {
    return impl.isImplementationOf(inter);
  }

  @Override
  public synchronized ClassHierarchy merge(final ClassHierarchy ch) {
    if (this == ch) {
      return this;
    }
    if (!(ch instanceof ClassHierarchyImpl)) {
      throw new UnsupportedOperationException("Can't merge java and non-java class hierarchies yet!");
    }
    if (this.jars.size() == 0) {
      return ch;
    }
    final ClassHierarchyImpl chi = (ClassHierarchyImpl) ch;
    final HashSet<URL> otherJars = new HashSet<>();
    otherJars.addAll(chi.jars);
    final HashSet<URL> myJars = new HashSet<>();
    myJars.addAll(this.jars);
    if (myJars.containsAll(otherJars)) {
      return this;
    } else if (otherJars.containsAll(myJars)) {
      return ch;
    } else {
      myJars.addAll(otherJars);
      return new ClassHierarchyImpl(myJars.toArray(new URL[0]));
    }
  }
}
