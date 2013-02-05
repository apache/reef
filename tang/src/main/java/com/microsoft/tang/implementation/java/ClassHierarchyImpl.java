package com.microsoft.tang.implementation.java;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.ClassNode;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.NamespaceNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.PackageNode;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Namespace;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.AbstractNode;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicMultiMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ClassHierarchyImpl implements ClassHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.

  private URLClassLoader loader;
  private final List<URL> jars;

  URL[] getJars() {
    return jars.toArray(new URL[0]);
  }

  public Class<?> classForName(String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }

  private final PackageNode namespace;

  private final class ClassComparator implements Comparator<Class<?>> {

    @Override
    public int compare(Class<?> arg0, Class<?> arg1) {
      return arg0.getName().compareTo(arg1.getName());
    }

  }

  private final Set<Class<?>> registeredClasses = new MonotonicSet<>(
      new ClassComparator());
  private final MonotonicMultiMap<ClassNode<?>, ClassNode<?>> knownImpls = new MonotonicMultiMap<>();
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicMap<>();

  public ClassHierarchyImpl(URL... jars) {
    this.namespace = JavaNodeFactory.createPackageNode();
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
  }

  public ClassHierarchyImpl(ClassLoader loader, URL... jars) {
    this.namespace = JavaNodeFactory.createPackageNode();
    this.jars = new ArrayList<URL>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, loader);
  }

  public void addJars(URL... j) {
    List<URL> newJars = new ArrayList<>();
    for (URL u : j) {
      if (!this.jars.contains(u)) {
        newJars.add(u);
        this.jars.add(u);
      }
    }
    // Note, URL class loader first looks in its parent, then in the array of
    // URLS passed in, in order. So, this line is equivalent to "reaching into"
    // URLClassLoader and adding the URLS to the end of the array.
    this.loader = new URLClassLoader(newJars.toArray(new URL[0]), this.loader);
  }

  @SuppressWarnings({ "unchecked", "unused" })
  private <T> NamespaceNode<T> registerNamespace(Namespace conf,
      ClassNode<T> classNode) throws BindException {
    String[] path = conf.value().split(ReflectionUtilities.regexp);
    Node root = namespace;
    // Search for the new node's parent, store it in root.
    for (int i = 0; i < path.length - 1; i++) {
      if (!root.contains(path[i])) {
        Node newRoot = JavaNodeFactory.createNamespaceNode(root, path[i]);
        root = newRoot;
      } else {
        root = root.get(path[i]);
        if (!(root instanceof NamespaceNode)) {
          throw new BindException("Attempt to register namespace inside of "
              + root + " namespaces and java packages/classes cannot overlap.");
        }
      }
    }
    if (root instanceof NamespaceNode) {
      Node target = ((NamespaceNode<?>) root).getTarget();
      if (target != null) {
        throw new BindException("Nested namespaces not implemented!");
      }
    }
    Node n = root.get(path[path.length - 1]);
    // n points to the new node (if it exists)
    NamespaceNode<T> ret;
    if (n == null) {
      ret = JavaNodeFactory.createNamespaceNode(root, path[path.length - 1],
          classNode);
    } else if (n instanceof NamespaceNode) {
      ret = (NamespaceNode<T>) n;
      ret.setTarget(classNode);
      for (Node child : ret.getChildren()) {
        if (true) {
          // TODO: implement + test nested namespaces.
          throw new BindException("Nested namespaces not implemented!");
        } else {
          // TODO: Better error message here. We're trying to merge two
          // namespaces. If put throws an exception, it probably found a
          // conflicting node name.
          try {
            ((AbstractNode) classNode).put(child);
          } catch (IllegalArgumentException e) {
            throw new BindException("Merging children of namespace "
                + ret.getFullName()
                + " failed.  Detected conflicting uses of name "
                + child.getFullName());
          }
        }
      }
    } else {
      throw new BindException("Attempt to register namespace on top of " + n
          + " namespaces and java packages/classes cannot overlap.");
    }
    return ret;
  }

  private <T, U> Node buildPathToNode(Class<U> clazz, boolean isPrefixTarget)
      throws BindException {
    String[] path = clazz.getName().split(ReflectionUtilities.regexp);
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
      return JavaNodeFactory.createClassNode(parent, clazz, isPrefixTarget);
    } else {
      if (isPrefixTarget) {
        throw new BindException(clazz
            + " cannot be both a namespace and parameter.");
      }
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
          throw new BindException("Named parameters " + oldNode.getFullName()
              + " and " + np.getFullName() + " have the same short name: "
              + shortName);
        }
        shortNames.put(shortName, np);
      }
      return np;
    }
  }

  private Node getNode(Class<?> clazz) throws NameResolutionException {
    return getNode(clazz.getName());
  }

  public Node getNode(String name) throws NameResolutionException {
    String[] path = name.split(ReflectionUtilities.regexp);
    return getNode(name, path, path.length);
  }

  private Node getNode(String name, String[] path, int depth)
      throws NameResolutionException {
    Node root = namespace;
    for (int i = 0; i < depth; i++) {
      if (root instanceof NamespaceNode) {
        NamespaceNode<?> ns = (NamespaceNode<?>) root;
        if (ns.getTarget() != null) {
          root = ns.getTarget();
        }
      }
      root = root.get(path[i]);
      if (root == null) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < i; j++) {
          sb.append(path[j]);
          if (j != i - 1) {
            sb.append(".");
          }
        }
        throw new NameResolutionException(name, sb.toString());
      }
    }
    return root;
  }

  private String arrayToDotString(String[] array, int length) {
    StringBuilder parentString = new StringBuilder(array[0]);
    for (int i = 1; i < length; i++) {
      parentString.append("." + array[i]);
    }
    return parentString.toString();
  }

  /**
   * Assumes parent packages are already registered.
   * 
   * @param packageName
   * @throws NameResolutionException
   */
  private void registerPackage(String[] packageName)
      throws NameResolutionException {

    try {
      getNode(arrayToDotString(packageName, packageName.length));
      return;
    } catch (NameResolutionException e) {
    }

    final PackageNode parent;
    if (packageName.length == 1) {
      parent = namespace;
    } else {
      parent = (PackageNode) getNode(arrayToDotString(packageName,
          packageName.length - 1));
    }
    JavaNodeFactory.createPackageNode(parent,
        packageName[packageName.length - 1]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.microsoft.tang.implementation.Namespace#register(java.lang.String)
   */
  @Override
  public Node register(String s) throws BindException {
    final Class<?> c;
    try {
      c = classForName(s);
    } catch (ClassNotFoundException e1) {
      return null;
    }
    try {
      Node n = getNode(c);
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
    Package pack = c.getPackage();
    if (pack != null) { // We're in an enclosing class, and we just registered
                        // it above!
      String[] packageList = pack.getName().split(ReflectionUtilities.regexp);
      for (int i = 0; i < packageList.length; i++) {
        try {
          registerPackage(Arrays.copyOf(packageList, i + 1));
        } catch (NameResolutionException e) {
          throw new IllegalStateException("Could not find parent package "
              + Arrays.toString(Arrays.copyOf(packageList, i + 1))
              + ", which this method should have registered.", e);
        }
      }
    }
    // Now, register the class. This has to be after the above so we know our
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
                throw new BindException(
                    "Incompatible argument type.  Constructor expects "
                        + arg.getType() + " but " + np.getName() + " is a "
                        + np.getFullArgName());
              }
            } catch (ClassNotFoundException e) {
              throw new BindException("Constructor refers to unknown class "
                  + arg.getType(), e);
            }
          }
        }
      }
    } else if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      register(np.getFullArgName());
      try {
        String defaultString = np.getDefaultInstanceAsString();
        if (defaultString != null) {
          defaultNamedParameterInstances.put(np, ReflectionUtilities.parse(
              classForName(np.getFullArgName()), defaultString));
        }
      } catch (ClassNotFoundException e) {
        throw new BindException("Named paramter " + np
            + " refers to unknown class " + np.getFullArgName(), e);
      }
    }
    return n;
  }

  final Map<NamedParameterNode<?>, Object> defaultNamedParameterInstances = new MonotonicMap<>();

  /**
   * Assumes that all of the parents of c have been registered already.
   * 
   * @param c
   */
  @SuppressWarnings("unchecked")
  private <T, U extends T> Node registerClass(final Class<U> c)
      throws BindException {
    if (c.isArray()) {
      throw new BindException("Can't register array types");
    }
    try {
      return getNode(c);
    } catch (NameResolutionException e) {
    }

    final Namespace nsAnnotation = c.getAnnotation(Namespace.class);
    final Node n;
    if (nsAnnotation == null) {
      n = buildPathToNode(c, false);
    } else {
      n = buildPathToNode(c, true);
      if (n instanceof NamedParameterNode) {
        throw new BindException("Found namespace annotation " + nsAnnotation
            + " with target " + n + " which is a named parameter.");
      }
      registerNamespace(nsAnnotation, (ClassNode<?>) n);
    }

    if (n instanceof ClassNode) {
      ClassNode<U> cn = (ClassNode<U>) n;
      Class<T> superclass = (Class<T>) c.getSuperclass();
      if (superclass != null) {
        try {
          putImpl((ClassNode<T>) getNode(superclass), cn);
        } catch (NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
      for (Class<?> interf : c.getInterfaces()) {
        try {
          putImpl((ClassNode<T>) getNode(interf), cn);
        } catch (NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    registeredClasses.add(c);
    return n;
  }

  private <T, U extends T> void putImpl(ClassNode<T> superclass,
      ClassNode<U> impl) {
    knownImpls.put(superclass, impl);
  }

  @SuppressWarnings("unchecked")
  <T> Set<ClassNode<T>> getKnownImpls(ClassNode<T> c) {
    return (Set<ClassNode<T>>) (Set<?>) knownImpls.getValuesForKey(c);
  }

  public Set<String> getRegisteredClassNames() {
    Set<String> s = new MonotonicSet<String>();
    for (Class<?> c : registeredClasses) {
      s.add(ReflectionUtilities.getFullName(c));
    }
    return s;
  }

  PackageNode getNamespace() {
    return namespace;
  }

  @Override
  public Collection<String> getShortNames() {
    return shortNames.keySet();
  }

  @Override
  public String resolveShortName(String shortName) {
    return shortNames.get(shortName).getFullName();
  }

  @Override
  public String toPrettyString() {
    return namespace.toIndentedString(0);
  }
}
