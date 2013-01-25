package com.microsoft.tang.implementation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Namespace;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicMultiMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class TypeHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.

  private URLClassLoader loader;
  private final List<URL> jars;

  public URL[] getJars() {
    return jars.toArray(new URL[0]);
  }

  Class<?> classForName(String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }

  private final PackageNode namespace;
  private final class ClassComparator implements Comparator<Class<?>> {

    @Override
    public int compare(Class<?> arg0, Class<?> arg1) {
      return arg0.getName().compareTo(arg1.getName());
    }
    
  }
  private final Set<Class<?>> registeredClasses = new MonotonicSet<Class<?>>(new ClassComparator());
  private final MonotonicMultiMap<ClassNode<?>, ClassNode<?>> knownImpls = new MonotonicMultiMap<ClassNode<?>, ClassNode<?>>();
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicMap<String, NamedParameterNode<?>>();

  public TypeHierarchy(URL... jars) {
    this.namespace = new PackageNode(null, "");
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
  }

  public TypeHierarchy(ClassLoader loader, URL... jars) {
    this.namespace = new PackageNode(null, "");
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
        Node newRoot = new NamespaceNode<T>(root, path[i]);
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
      ret = new NamespaceNode<T>(root, path[path.length - 1], classNode);
    } else if (n instanceof NamespaceNode) {
      ret = (NamespaceNode<T>) n;
      ret.setTarget(classNode);
      for (Node child : ret.children.values()) {
        if (true) {
          // TODO: implement + test nested namespaces.
          throw new BindException("Nested namespaces not implemented!");
        } else {
          // TODO: Better error message here. We're trying to merge two
          // namespaces. If put throws an exception, it probably found a
          // conflicting node name.
          try {
            classNode.put(child);
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
      return new ClassNode<U>(parent, clazz, isPrefixTarget);
    } else {
      if (isPrefixTarget) {
        throw new BindException(clazz
            + " cannot be both a namespace and parameter.");
      }
      @SuppressWarnings("unchecked")
      // checked inside of NamedParameterNode, using reflection.
      NamedParameterNode<T> np = new NamedParameterNode<T>(parent,
          (Class<? extends Name<T>>) clazz, (Class<T>) argType);
      String shortName = np.getShortName();
      if (shortName != null) {
        NamedParameterNode<?> oldNode = shortNames.get(shortName);
        if (oldNode != null) {
          if (oldNode.getNameClass() == np.getNameClass()) {
            throw new IllegalStateException("Tried to double bind "
                + oldNode.getNameClass() + " to short name " + shortName);
          }
          throw new BindException("Named parameters " + oldNode.getNameClass()
              + " and " + np.getNameClass() + " have the same short name: "
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
        if (ns.target != null) {
          root = ns.target;
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
    new PackageNode(parent, packageName[packageName.length - 1]);
  }

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
    if(enclosing != null) {
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
          register(ReflectionUtilities.getFullName(arg.type));
          if (arg.name != null) {
            NamedParameterNode<?> np = (NamedParameterNode<?>) register(ReflectionUtilities.getFullName(arg.name
                .value()));
            if (!ReflectionUtilities.isCoercable(arg.type, np.getArgClass())) {
              throw new BindException(
                  "Incompatible argument type.  Constructor expects "
                      + arg.type + " but " + np.getName() + " is a "
                      + np.getArgClass());
            }
          }
        }
      }
    } else if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      register(ReflectionUtilities.getFullName(np.getArgClass()));
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
    for(Class<?> c : registeredClasses) {
      s.add(ReflectionUtilities.getFullName(c));
    }
    return s;
  }

  public PackageNode getNamespace() {
    return namespace;
  }

  public Collection<String> getShortNames() {
    return shortNames.keySet();
  }
  public String resolveShortName(String shortName) {
    return ((NamedParameterNode<?>) shortNames.get(shortName)).getFullName();
  }

  /**
   * TODO: Fix up output of TypeHierarchy!
   * 
   * @return
   */
  public String toPrettyString() {
    return namespace.toIndentedString(0);
  }

  public abstract class Node implements Comparable<Node> {
    protected final Node parent;
    protected final String name;

    @Override
    public boolean equals(Object o) {
      Node n = (Node) o;
      final boolean parentsEqual;
      if (n.parent == this.parent) {
        parentsEqual = true;
      } else if (n.parent == null) {
        parentsEqual = false;
      } else {
        parentsEqual = n.parent.equals(this.parent);
      }
      if (!parentsEqual) {
        return false;
      }
      return this.name.equals(n.name);
    }

    public String getFullName() {
      final String ret;
      if (parent == null) {
        if (name == "") {
          ret = "[root node]";
        } else {
          throw new IllegalStateException(
              "can't have node with name and null parent!");
        }
      } else {
        String parentName = parent.getFullName();
        if (parentName.length() == 0) {
          ret = name;
        } else {
          ret = parent.getFullName() + "." + name;
        }
      }
      if (ret.length() == 0) {
        throw new IllegalStateException(
            "getFullName() ended up with an empty string!");
      }
      return ret;
    }

    public Map<String, Node> children = new MonotonicMap<String, Node>();

    Node(Node parent, Class<?> name) {
      this.parent = parent;
      this.name = ReflectionUtilities.getSimpleName(name);
      if (this.name.length() == 0) {
        throw new IllegalArgumentException(
            "Zero length ClassName means bad news");
      }
      if (parent != null) {
        parent.put(this);
      }
    }

    Node(Node parent, String name) {
      this.parent = parent;
      this.name = name;
      if (parent != null) {
        if (name.length() == 0) {
          throw new IllegalArgumentException(
              "Zero length child name means bad news");
        }
        parent.put(this);
      }
    }

    public boolean contains(String key) {
      return children.containsKey(key);
    }

    public Node get(String key) {
      return children.get(key);
    }

    void put(Node n) {
      children.put(n.name, n);
    }

    public String toIndentedString(int level) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < level; i++) {
        sb.append("\t");
      }
      sb.append(toString() + "\n");
      if (children != null) {
        for (Node n : children.values()) {
          sb.append(n.toIndentedString(level + 1));
        }
      }
      return sb.toString();
    }

    @Override
    public String toString() {
      return "[" + ReflectionUtilities.getFullName(this.getClass()) + " '"
          + getFullName() + "']";
    }

    public String getName() {
      return name;
    }

    @Override
    public int compareTo(Node n) {
      return getFullName().compareTo(n.getFullName());
    }
  }

  public class PackageNode extends Node {
    PackageNode(Node parent, String name) {
      super(parent, name);
    }
  }

  @SuppressWarnings("unchecked")
  public class ClassNode<T> extends Node {
    private final Class<T> clazz;
    private final boolean isPrefixTarget;
    private final ConstructorDef<T>[] injectableConstructors;

    public Class<T> getClazz() {
      return clazz;
    }

    public boolean getIsPrefixTarget() {
      return isPrefixTarget;
    }

    public ConstructorDef<T>[] getInjectableConstructors() {
      return injectableConstructors;
    }

    @Override
    public String getFullName() {
      if (clazz.isPrimitive()) {
        return super.getFullName();
      } else {
        return clazz.getName();
      }
    }

    public boolean isInjectionCandidate() {
      final boolean injectable;
      if (clazz.isLocalClass() || clazz.isMemberClass()) {
        if (!Modifier.isStatic(clazz.getModifiers())) {
          injectable = false;
        } else {
          injectable = true;
        }
      } else {
        injectable = true;
      }
      return injectable;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString() + ": ");
      if (getInjectableConstructors() != null) {
        for (ConstructorDef<T> c : getInjectableConstructors()) {
          sb.append(c.toString() + ", ");
        }
      } else {
        sb.append("OBJECT BUILD IN PROGRESS!  BAD NEWS!");
      }
      return sb.toString();
    }

    public ConstructorDef<T> createConstructorDef(Class<?>... paramTypes)
        throws BindException {
      if (!isInjectionCandidate()) {
        throw new BindException(
            "Cannot @Inject non-static member/local class: " + clazz);
      }
      try {
        return createConstructorDef(clazz.getConstructor(paramTypes));
      } catch (NoSuchMethodException e) {
        throw new BindException(
            "Could not find requested constructor for class " + clazz, e);
      }
    }

    private ConstructorDef<T> createConstructorDef(Constructor<T> constructor)
        throws BindException {
      // We don't support non-static member classes with @Inject annotations.
      if (!isInjectionCandidate()) {
        throw new BindException(
            "Cannot @Inject non-static member/local class: " + clazz);
      }
      Class<?>[] paramTypes = constructor.getParameterTypes();
      Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
      if (paramTypes.length != paramAnnotations.length) {
        throw new IllegalStateException();
      }
      ConstructorArg[] args = new ConstructorArg[paramTypes.length];
      for (int i = 0; i < paramTypes.length; i++) {
        // if there is an appropriate annotation, use that.
        Parameter named = null;
        for (int j = 0; j < paramAnnotations[i].length; j++) {
          Annotation annotation = paramAnnotations[i][j];
          if (annotation instanceof Parameter) {
            named = (Parameter) annotation;
          }
        }
        args[i] = new ConstructorArg(paramTypes[i], named);
      }
      try {
        return new ConstructorDef<T>(args, constructor);
      } catch (BindException e) {
        throw new BindException("Detected bad constructor in " + constructor
            + " in " + clazz, e);
      }
    }

    public ClassNode(Node parent, Class<T> clazz, boolean isPrefixTarget) throws BindException {
      super(parent, clazz);
      this.clazz = clazz;
      this.isPrefixTarget = isPrefixTarget;

      Constructor<T>[] constructors = (Constructor<T>[]) clazz
          .getDeclaredConstructors();
      MonotonicSet<ConstructorDef<T>> injectableConstructors = new MonotonicSet<ConstructorDef<T>>();

      for (int k = 0; k < constructors.length; k++) {

        if (constructors[k].getAnnotation(Inject.class) != null) {
          // go through the constructor arguments.
          if (constructors[k].isSynthetic()) {
            // Not sure if we *can* unit test this one.
            throw new IllegalStateException(
                "Synthetic constructor was annotated with @Inject!");
          }

          // ConstructorDef's constructor checks for duplicate
          // parameters
          // The injectableConstructors set checks for ambiguous
          // boundConstructors.
          ConstructorDef<T> def = createConstructorDef(constructors[k]);
          if (injectableConstructors.contains(def)) {
            throw new BindException(
                "Ambiguous boundConstructors detected in class " + clazz + ": "
                    + def + " differs from some other " + " constructor only "
                    + "by parameter order.");
          } else {
            injectableConstructors.add(def);
          }
        }
      }
      this.injectableConstructors = injectableConstructors
          .toArray((ConstructorDef<T>[]) new ConstructorDef[0]);
    }
  }

  public class NamespaceNode<T> extends Node {
    private ClassNode<T> target;

    public NamespaceNode(Node parent, String name, ClassNode<T> target) {
      super(parent, name);
      if (target != null && (!target.isPrefixTarget)) {
        throw new IllegalStateException();
      }
      this.target = target;
    }

    public NamespaceNode(Node parent, String name) {
      super(parent, name);
    }

    public void setTarget(ClassNode<T> target) {
      if (this.target != null) {
        throw new IllegalStateException("Attempt to set namespace target from "
            + this.target + " to " + target);
      }
      this.target = target;
      if (!target.isPrefixTarget) {
        throw new IllegalStateException();
      }
    }

    public Node getTarget() {
      return target;
    }

    @Override
    public String toString() {
      if (target != null) {
        return super.toString() + " -> " + target.toString();
      } else {
        return super.toString();
      }
    }

  }

  public class NamedParameterNode<T> extends Node {
    private final Class<? extends Name<T>> nameClass;
    private final NamedParameter namedParameter;
    private final Class<T> argClass;
    private final T defaultInstance;

    NamedParameterNode(Node parent, Class<? extends Name<T>> clazz,
        Class<T> argClass) throws BindException {
      super(parent, clazz);
      this.nameClass = clazz;
      this.namedParameter = clazz.getAnnotation(NamedParameter.class);
      this.argClass = argClass;
      if (this.namedParameter == null
          || namedParameter.default_value().length() == 0) {
        this.defaultInstance = null;
      } else {
        try {
          this.defaultInstance = ReflectionUtilities.parse(this.getArgClass(),
              namedParameter.default_value());
        } catch (UnsupportedOperationException e) {
          throw new BindException("Could not register NamedParameterNode for "
              + clazz.getName() + ".  Default value "
              + namedParameter.default_value() + " failed to parse.", e);
        }
      }
    }

    @Override
    public String toString() {
      String ret = ReflectionUtilities.getSimpleName(getArgClass());
      if (namedParameter == null) {
        ret = ret + " " + name;
      } else {
        ret = ret + " " + name;
      }
      return ret;
    }

    @Override
    public String getFullName() {
      return getNameClass().getName();
    }

    public Class<T> getArgClass() {
      return argClass;
    }

    public Class<? extends Name<T>> getNameClass() {
      return nameClass;
    }

    public T getDefaultInstance() {
      return defaultInstance;
    }

    public String getDocumentation() {
      if (namedParameter != null) {
        return namedParameter.doc();
      } else {
        return "";
      }
    }

    /*
     * public NamedParameter getNamedParameter() { return namedParameter; }
     */
    public String getShortName() {
      if (namedParameter.short_name() != null
          && namedParameter.short_name().length() == 0) {
        return null;
      }
      return namedParameter.short_name();
    }

  }

  public class ConstructorArg {
    private final Class<?> type;
    private final Parameter name;

    public String getName() {
      return name == null ? type.getName() : name.value().getName();
    }

    ConstructorArg(Class<?> type, Parameter name) {
      this.type = type;
      this.name = name;
    }

    @Override
    public String toString() {
      return name == null ? ReflectionUtilities.getFullName(type)
          : ReflectionUtilities.getFullName(type) + " "
              + ReflectionUtilities.getFullName(name.value());
    }

    @Override
    public boolean equals(Object o) {
      ConstructorArg arg = (ConstructorArg) o;
      if (!type.equals(arg.type)) {
        return false;
      }
      if (name == null && arg.name == null) {
        return true;
      }
      if (name == null && arg.name != null) {
        return false;
      }
      if (name != null && arg.name == null) {
        return false;
      }
      return name.equals(arg.name);

    }
  }

  public class ConstructorDef<T> implements Comparable<ConstructorDef<?>> {
    private final ConstructorArg[] args;
    private final Constructor<T> constructor;

    public ConstructorArg[] getArgs() {
      return args;
    }

    Constructor<T> getConstructor() {
      return constructor;
    }

    @Override
    public String toString() {
      if (getArgs().length == 0) {
        return "()";
      }
      StringBuilder sb = new StringBuilder("(" + getArgs()[0]);
      for (int i = 1; i < getArgs().length; i++) {
        sb.append("," + getArgs()[i]);
      }
      sb.append(")");
      return sb.toString();
    }

    ConstructorDef(ConstructorArg[] args, Constructor<T> constructor)
        throws BindException {
      this.args = args;
      this.constructor = constructor;
      constructor.setAccessible(true);

      for (int i = 0; i < this.getArgs().length; i++) {
        for (int j = i + 1; j < this.getArgs().length; j++) {
          if (this.getArgs()[i].equals(this.getArgs()[j])) {
            throw new BindException(
                "Repeated constructor parameter detected.  "
                    + "Cannot inject constructor" + constructor);
          }
        }
      }
    }

    /**
     * Check to see if two boundConstructors take indistinguishable arguments.
     * If so (and they are in the same class), then this would lead to ambiguous
     * injection targets, and we want to fail fast.
     * 
     * TODO could be faster. Currently O(n^2) in number of parameters.
     * 
     * @param def
     * @return
     */
    boolean equalsIgnoreOrder(ConstructorDef<?> def) {
      if (getArgs().length != def.getArgs().length) {
        return false;
      }
      for (int i = 0; i < getArgs().length; i++) {
        boolean found = false;
        for (int j = 0; j < getArgs().length; j++) {
          if (getArgs()[i].getName().equals(getArgs()[j].getName())) {
            found = true;
          }
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean equals(Object o) {
      return equalsIgnoreOrder((ConstructorDef<?>) o);
    }

    public boolean isMoreSpecificThan(ConstructorDef<?> def) {
      for (int i = 0; i < getArgs().length; i++) {
        boolean found = false;
        for (int j = 0; j < def.getArgs().length; j++) {
          if (getArgs()[i].equals(def.getArgs()[j])) {
            found = true;
          }
        }
        if (found == false)
          return false;
      }
      return getArgs().length > def.getArgs().length;
    }

    @Override
    public int compareTo(ConstructorDef<?> o) {
      return getConstructor().toString().compareTo(o.getConstructor().toString());
    }
  }
}
