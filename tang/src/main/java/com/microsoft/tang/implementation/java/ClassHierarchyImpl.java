package com.microsoft.tang.implementation.java;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.annotations.Name;
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
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ClassHierarchyImpl implements JavaClassHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.

  private final URLClassLoader loader;
  private final List<URL> jars;

  private final PackageNode namespace;
  private final TreeSet<String> registeredClasses = new MonotonicSet<>();
  // Note: this is only used to sanity check short names (so that name clashes get resolved).
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicMap<>();

  public final ParameterParser parameterParser = new ParameterParser();

  @Override
  public <T> Object parseDefaultValue(NamedParameterNode<T> name) {
    String val = name.getDefaultInstanceAsString();
    if (val != null) {
      try {
        return parse(name, val);
      } catch (ParseException e) {
        throw new ClassHierarchyException("Could not parse default value", e);
      }
    } else {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Object parse(NamedParameterNode<T> np, String value) throws ParseException {
    try {
      final ClassNode<T> iface = (ClassNode<T>)getNode(np.getFullArgName());
      try {
        return parameterParser.parse(iface.getFullName(), value);
      } catch (UnsupportedOperationException e) {
        try {
          final Node impl = getNode(value);
          if(impl instanceof ClassNode) {
            if(isImplementation(iface, (ClassNode<?>)impl)) {
              return (T)impl;
            }
          }
          throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() + " cannot take non-subclass " + impl.getFullName());
        } catch(NameResolutionException e2) {
          throw new ParseException("Name<" + iface.getFullName() + "> " + np.getFullName() + " cannot take non-class " + value);
        }
      }
    } catch(NameResolutionException e) {
      throw new IllegalStateException("Could not parse validated named parameter argument type.  NamedParameter is " + np.getFullName() + " argument type is " + np.getFullArgName());
    }
  }

  @Override
  public Class<?> classForName(String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }

  public ClassHierarchyImpl(URL... jars) {
    this.namespace = JavaNodeFactory.createRootPackageNode();
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
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

    Class<?> argType = ReflectionUtilities.getNamedParameterTargetOrNull(clazz);

    if (argType == null) {
      return JavaNodeFactory.createClassNode(parent, clazz);
    } else {
      @SuppressWarnings("unchecked")
      // checked inside of NamedParameterNode, using reflection.
      NamedParameterNode<T> np = JavaNodeFactory.createNamedParameterNode(
          parent, (Class<? extends Name<T>>) clazz, (Class<T>) argType);
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
              if (!ReflectionUtilities.isCoercable(classForName(arg.getType()),
                  classForName(np.getFullArgName()))) {
                throw new ClassHierarchyException(
                    "Named parameter type mismatch.  Constructor expects a "
                        + arg.getType() + " but " + np.getName() + " is a "
                        + np.getFullArgName());
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
  private <T, U extends T> Node registerClass(final Class<U> c)
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
      ClassNode<U> cn = (ClassNode<U>) n;
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
    registeredClasses.add(ReflectionUtilities.getFullName(c));
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
