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
package com.microsoft.tang.implementation.java;

import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.ParseException;
import com.microsoft.tang.formats.ParameterParser;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.MonotonicTreeMap;
import com.microsoft.tang.util.ReflectionUtilities;

public class ClassHierarchyImpl implements JavaClassHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.  Tint works around this using such
  // a library.

  /** The classloader that was used to populate this class hierarchy. */
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
  /**
   * The ParameterParser that this ClassHierarchy uses to parse default values.
   * Custom parameter parsers allow applications to extend the set of classes
   * that Tang can parse.
   */
  public final ParameterParser parameterParser = new ParameterParser();

  /**
   * A helper method that returns the parsed default value of a given
   * NamedParameter.
   * @return null or an empty set if there is no default value, the default value (or set of values) otherwise.
   * @throws ClassHierarchyException if a default value was specified, but could not be parsed, or if a set of
   *         values were specified for a non-set parameter.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T parseDefaultValue(NamedParameterNode<T> name) {
    String[] vals = name.getDefaultInstanceAsStrings();
    T[] ret = (T[]) new Object[vals.length];
    for(int i = 0; i < vals.length; i++) {
      String val = vals[i];
      try {
        ret[i] = parse(name, val);
      } catch (ParseException e) {
        throw new ClassHierarchyException("Could not parse default value", e);
      }
    }
    if(name.isSet()) {
      return (T)new HashSet<T>(Arrays.asList(ret));
    } else if(name.isList()) {
      return (T)new ArrayList<T>(Arrays.asList(ret));
    } else {
      if(ret.length == 0) {
        return null;
      } else if(ret.length == 1) {
        return ret[0];
      } else {
        throw new IllegalStateException("Multiple defaults for non-set named parameter! " + name.getFullName());
      }
    }
  }

  /**
   * Parse a string, assuming that it is of the type expected by a given NamedParameter.
   * 
   * This method does not deal with sets; if the NamedParameter is set valued, then the provided
   * string should correspond to a single member of the set.  It is up to the caller to call parse
   * once for each value that should be parsed as a member of the set.
   * 
   * @return a non-null reference to the parsed value.
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T parse(NamedParameterNode<T> np, String value) throws ParseException {
    final ClassNode<T> iface;
    try {
      iface = (ClassNode<T>)getNode(np.getFullArgName());
    } catch(NameResolutionException e) {
      throw new IllegalStateException("Could not parse validated named parameter argument type.  NamedParameter is " + np.getFullName() + " argument type is " + np.getFullArgName());
    }
    Class<?> clazz;
    String fullName;
    try {
      clazz = (Class<?>)classForName(iface.getFullName());
      fullName = null;
    } catch(ClassNotFoundException e) {
      clazz = null;
      fullName = iface.getFullName();
    }
    try {
      if(clazz != null) {
        return (T)parameterParser.parse(clazz, value);
      } else {
        return parameterParser.parse(fullName, value);
      }
    } catch (UnsupportedOperationException e) {
      try {
        final Node impl = getNode(value);
        if(impl instanceof ClassNode) {
          if(isImplementation(iface, (ClassNode<?>)impl)) {
            return (T)impl;
          }
        }
        throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() + " cannot take non-subclass " + impl.getFullName(), e);
      } catch(NameResolutionException e2) {
        throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() + " cannot take non-class " + value, e);
      }
    }
  }

  /**
   * Helper method that converts a String to a Class using this
   * ClassHierarchy's classloader.
   */
  @Override
  public Class<?> classForName(String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }
  @SuppressWarnings("unchecked")
  public ClassHierarchyImpl() {
    this(new URL[0], new Class[0]);
  }
  @SuppressWarnings("unchecked")
  public ClassHierarchyImpl(URL... jars) {
    this(jars, new Class[0]);
  }
  
  public ClassHierarchyImpl(URL[] jars, Class<? extends ExternalConstructor<?>>[] parameterParsers) {
    this.namespace = JavaNodeFactory.createRootPackageNode();
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
    for(Class<? extends ExternalConstructor<?>> p : parameterParsers) {
      try {
        parameterParser.addParser(p);
      } catch (BindException e) {
        throw new IllegalArgumentException("Could not register parameter parsers", e);
      }
    }
  }
  private <T, U> Node buildPathToNode(Class<U> clazz)
      throws ClassHierarchyException {
    String[] path = clazz.getName().split("\\$");
    
    Node root = namespace;
    for (int i = 0; i < path.length - 1; i++) {
      root = root.get(path[i]);
    }

    if (root == null) {
      throw new NullPointerException();
    }
    Node parent = root;

    Type argType = ReflectionUtilities.getNamedParameterTargetOrNull(clazz);

    if (argType == null) {
      return JavaNodeFactory.createClassNode(parent, clazz);
    } else {
      
      @SuppressWarnings("unchecked")
      // checked inside of NamedParameterNode, using reflection.
      NamedParameterNode<T> np = JavaNodeFactory.createNamedParameterNode(
          parent, (Class<? extends Name<T>>) clazz, argType);
      
      if(parameterParser.canParse(ReflectionUtilities.getFullName(argType))) {
        if(clazz.getAnnotation(NamedParameter.class).default_class() != Void.class) {
          throw new ClassHierarchyException("Named parameter " + ReflectionUtilities.getFullName(clazz) + " defines default implementation for parsable type " + ReflectionUtilities.getFullName(argType));
        }
      }

      String shortName = np.getShortName();
      if (shortName != null) {
        NamedParameterNode<?> oldNode = shortNames.get(shortName);
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

  private Node getAlreadyBoundNode(Class<?> clazz) throws NameResolutionException {
    return getAlreadyBoundNode(ReflectionUtilities.getFullName(clazz));
  }
  @Override
  public Node getNode(Class<?> clazz) {
    try {
      return getNode(ReflectionUtilities.getFullName(clazz));
    } catch(NameResolutionException e) {
      throw new ClassHierarchyException("JavaClassHierarchy could not resolve " + clazz 
          + " which is definitely avalable at runtime", e);
    }
  }
  @Override
  public synchronized Node getNode(String name) throws NameResolutionException {
    Node n = register(name);
    if(n == null) {
      // This will never succeed; it just generates a nice exception.
      getAlreadyBoundNode(name);
      throw new IllegalStateException("IMPLEMENTATION BUG: Register failed, "
        + "but getAlreadyBoundNode succeeded!");
    }
    return n;
  }
  private Node getAlreadyBoundNode(String name) throws NameResolutionException {
    Node root = namespace;
    String[] toks = name.split("\\$");
    String outerClassName = toks[0];
    root = root.get(outerClassName);
    if(root == null) {
      throw new NameResolutionException(name, outerClassName);
    }
    for (int i = 1; i < toks.length; i++) {
      root = root.get(toks[i]);
      if (root == null) {
        StringBuilder sb = new StringBuilder(outerClassName);
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

  private Node register(String s) {
    final Class<?> c;
    try {
      c = classForName(s);
    } catch (ClassNotFoundException e1) {
      return null;
    }
    try {
      Node n = getAlreadyBoundNode(c);
      return n;
    } catch (NameResolutionException e) {
    }
    // First, walk up the class hierarchy, registering all out parents. This
    // can't be loopy.
    if (c.getSuperclass() != null) {
      register(ReflectionUtilities.getFullName(c.getSuperclass()));
    }
    for (Class<?> i : c.getInterfaces()) {
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
    Class<?> enclosing = c.getEnclosingClass();
    if (enclosing != null) {
      register(ReflectionUtilities.getFullName(enclosing));
    }

    // Now register the class. This has to be after the above so we know our
    // parents (superclasses and enclosing packages) are already registered.
    Node n = registerClass(c);

    // Finally, do things that might introduce cycles that invlove c.
    // This has to be below registerClass, which ensures that any cycles
    // this stuff introduces are broken.
    for (Class<?> inner_class : c.getDeclaredClasses()) {
      register(ReflectionUtilities.getFullName(inner_class));
    }
    if (n instanceof ClassNode) {
      ClassNode<?> cls = (ClassNode<?>) n;
      for (ConstructorDef<?> def : cls.getInjectableConstructors()) {
        for (ConstructorArg arg : def.getArgs()) {
          register(arg.getType());
          if (arg.getNamedParameterName() != null) {
            NamedParameterNode<?> np = (NamedParameterNode<?>) register(arg
                .getNamedParameterName());
            try {
              if(np.isSet()) {
                /// XXX When handling sets, need to track target of generic parameter, and check the type here!
              } else if(np.isList()) {

              } else {
                if (!ReflectionUtilities.isCoercable(classForName(arg.getType()),
                    classForName(np.getFullArgName()))) {
                  throw new ClassHierarchyException(
                      "Named parameter type mismatch in " + cls.getFullName() + ".  Constructor expects a "
                          + arg.getType() + " but " + np.getName() + " is a "
                          + np.getFullArgName());
                }
              }
            } catch (ClassNotFoundException e) {
              throw new ClassHierarchyException("Constructor refers to unknown class "
                  + arg.getType(), e);
            }
          }
        }
      }
    } else if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
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
    } catch (NameResolutionException e) {
    }

    final Node n = buildPathToNode(c);

    if (n instanceof ClassNode) {
      ClassNode<T> cn = (ClassNode<T>) n;
      Class<T> superclass = (Class<T>) c.getSuperclass();
      if (superclass != null) {
        try {
          ((ClassNode<T>) getAlreadyBoundNode(superclass)).putImpl(cn);
        } catch (NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
      for (Class<?> interf : c.getInterfaces()) {
        try {
          ((ClassNode<T>) getAlreadyBoundNode(interf)).putImpl(cn);
        } catch (NameResolutionException e) {
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

  @Override
  public synchronized boolean isImplementation(ClassNode<?> inter, ClassNode<?> impl) {
    return impl.isImplementationOf(inter);
  }

  @Override
  public synchronized ClassHierarchy merge(ClassHierarchy ch) {
    if(this == ch) { return this; }
    if(!(ch instanceof ClassHierarchyImpl)) {
      throw new UnsupportedOperationException("Can't merge java and non-java class hierarchies yet!");
    }
    if(this.jars.size() == 0) {
      return ch;
    }
    ClassHierarchyImpl chi = (ClassHierarchyImpl)ch;
    HashSet<URL> otherJars = new HashSet<>();
    otherJars.addAll(chi.jars);
    HashSet<URL> myJars = new HashSet<>();
    myJars.addAll(this.jars);
    if(myJars.containsAll(otherJars)) {
      return this;
    } else if(otherJars.containsAll(myJars)) {
      return ch;
    } else {
      myJars.addAll(otherJars);
      return new ClassHierarchyImpl(myJars.toArray(new URL[0]));
    }
  }
}
