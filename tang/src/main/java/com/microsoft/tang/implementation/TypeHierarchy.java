package com.microsoft.tang.implementation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
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

  private final PackageNode namespace;
  private final Set<Class<?>> registeredClasses = new MonotonicSet<Class<?>>();
  private final MonotonicMultiMap<ClassNode<?>, ClassNode<?>> knownImpls = new MonotonicMultiMap<ClassNode<?>, ClassNode<?>>();
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicMap<String, NamedParameterNode<?>>();
  
  public TypeHierarchy() {
    namespace = new PackageNode(null, "");
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

  /**
   * @param clazz
   * @return T if clazz implements Name<T>, null otherwise
   * @throws BindException
   *           If clazz's definition incorrectly uses Name or @NamedParameter
   */
  private Class<?> getNamedParameterTargetOrNull(Class<?> clazz)
      throws BindException {
    Annotation npAnnotation = clazz.getAnnotation(NamedParameter.class);
    boolean hasSuperClass = (clazz.getSuperclass() != Object.class);

    boolean isInjectable = false;
    boolean hasConstructor = false;
    // TODO Figure out how to properly differentiate between default and
    // non-default zero-arg constructors?
    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    if (constructors.length > 1) {
      hasConstructor = true;
    }
    if (constructors.length == 1) {
      Constructor<?> c = constructors[0];
      Class<?>[] p = c.getParameterTypes();
      if (p.length > 1) {
        // Multiple args. Definitely not implicit.
        hasConstructor = true;
      } else if (p.length == 1) {
        // One arg. Could be an inner class, in which case the compiler
        // included an implicit one parameter constructor that takes the
        // enclosing type.
        if (p[0] != clazz.getEnclosingClass()) {
          hasConstructor = true;
        }
      }
    }
    for (Constructor<?> c : constructors) {
      for (Annotation a : c.getDeclaredAnnotations()) {
        if (a instanceof Inject) {
          isInjectable = true;
        }
      }
    }

    Class<?>[] allInterfaces = clazz.getInterfaces();
    Type[] interfaces = clazz.getGenericInterfaces();

    boolean hasMultipleInterfaces = (allInterfaces.length > 1);
    boolean implementsName = false;
    Class<?> parameterClass = null;
    for (Type genericNameType : interfaces) {
      if (genericNameType instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) genericNameType;
        if (ptype.getRawType() == Name.class) {
          implementsName = true;
          Type t = ptype.getActualTypeArguments()[0];
          // It could be that the parameter is, itself a generic type. Not
          // sure if we should support this, but we do for now.
          if (t instanceof ParameterizedType) {
            // Get the underlying raw type of the parameter.
            t = ((ParameterizedType) t).getRawType();
          }
          parameterClass = (Class<?>) t;
        }
      }
    }
    if (npAnnotation == null) {
      if (implementsName) {
        throw new BindException(clazz
            + " is missing its @NamedParameter annotation.");
      } else {
        return null;
      }
    } else {
      if (!implementsName) {
        throw new BindException("Found illegal @NamedParameter " + clazz
            + " does not implement name");
      }
      if (hasSuperClass) {
        throw new BindException("Named parameter " + clazz
            + " has a superclass other than Object.");
      }
      if (hasConstructor || isInjectable) {
        throw new BindException("Named parameter " + clazz + " has "
            + (isInjectable ? "an injectable" : "a") + " constructor. "
            + " Name parameters must not delcare any constructors.");
      }
      if (hasMultipleInterfaces) {
        throw new BindException("Named parameter " + clazz + " implements "
            + "multiple interfaces.  It is only allowed to implement Name<T>");
      }
      if (parameterClass == null) {
        throw new BindException(
            "Missing type parameter in named parameter declaration.  " + clazz
                + " implements raw type Name, but must implement"
                + " generic type Name<T>.");
      }
      return parameterClass;
    }
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

    Class<?> argType = getNamedParameterTargetOrNull(clazz);

    if (argType == null) {
      return new ClassNode<U>(parent, clazz, isPrefixTarget, false);
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

  public Node getNode(Class<?> clazz) throws NameResolutionException {
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

  public Node register(Class<?> c) throws BindException {
    if (c == null) {
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
      register(c.getSuperclass());
    }
    for (Class<?> i : c.getInterfaces()) {
      register(i);
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
    register(c.getEnclosingClass());

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
      register(inner_class);
    }
    if (n instanceof ClassNode) {
      ClassNode<?> cls = (ClassNode<?>) n;
      for (ConstructorDef<?> def : cls.injectableConstructors) {
        for (ConstructorArg arg : def.args) {
          register(arg.type);
          if (arg.name != null) {
            NamedParameterNode<?> np = (NamedParameterNode<?>) register(arg.name
                .value());
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
      register(np.argClass);
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

  public Set<Class<?>> getRegisteredClasses() {
    return registeredClasses;
  }

  public PackageNode getNamespace() {
    return namespace;
  }

  public Collection<NamedParameterNode<?>> getNamedParameterNodes() {
    return shortNames.values();
  }

  @SuppressWarnings("unchecked")
  public <T> NamedParameterNode<T> getNodeFromShortName(String shortName) {
    return (NamedParameterNode<T>) shortNames.get(shortName);
  }

  /**
   * TODO: Fix up output of TypeHierarchy!
   * 
   * @return
   */
  public String toPrettyString() {
    return namespace.toIndentedString(0);
  }

  public abstract class Node {
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

    String getFullName() {
      if (parent == null) {
        return name;
      } else {
        String parentName = parent.getFullName();
        if(parentName.length() == 0) {
          return name;
        } else {
          return parent.getFullName() + "." + name;
        }
      }
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
      return "[" + ReflectionUtilities.getSimpleName(this.getClass()) + " '" + getFullName()
          + "']";
    }

    public String getType() {
      return ReflectionUtilities.getSimpleName(this.getClass());
    }

    public String getName() {
      return name;
    }

    public Collection<Node> getChildren() {
      return children.values();
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
    private boolean isSingleton;
    public final ConstructorDef<T>[] injectableConstructors;

    public Class<T> getClazz() {
      return clazz;
    }

    public boolean getIsPrefixTarget() {
      return isPrefixTarget;
    }

    public void setIsSingleton() {
      isSingleton = true;
    }

    public boolean getIsSingleton() {
      return isSingleton;
    }

    public ConstructorDef<T>[] getInjectableConstructors() {
      return injectableConstructors;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString() + ": ");
      if (injectableConstructors != null) {
        for (ConstructorDef<T> c : injectableConstructors) {
          sb.append(c.toString() + ", ");
        }
      } else {
        sb.append("OBJECT BUILD IN PROGRESS!  BAD NEWS!");
      }
      return sb.toString();
    }

    public ClassNode(Node parent, Class<T> clazz, boolean isPrefixTarget,
        boolean isSingleton) throws BindException {
      super(parent, clazz);
      this.clazz = clazz;
      this.isPrefixTarget = isPrefixTarget;
      this.isSingleton = isSingleton;

      // Don't support non-static member classes with @Inject annotations.
      boolean injectable = true;
      if (clazz.isLocalClass() || clazz.isMemberClass()) {
        if (!Modifier.isStatic(clazz.getModifiers())) {
          injectable = false;
        }
      }

      Constructor<T>[] constructors = (Constructor<T>[]) clazz
          .getDeclaredConstructors();
      MonotonicSet<ConstructorDef<T>> injectableConstructors = new MonotonicSet<ConstructorDef<T>>();

      for (int k = 0; k < constructors.length; k++) {

        if (constructors[k].getAnnotation(Inject.class) != null) {
          if (!injectable) {
            throw new BindException(
                "Cannot @Inject non-static member/local class: " + clazz);
          }
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
          Class<?>[] paramTypes = constructors[k].getParameterTypes();
          Annotation[][] paramAnnotations = constructors[k]
              .getParameterAnnotations();
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
          ConstructorDef<T> def = new ConstructorDef<T>(args, constructors[k]);
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
    final Class<? extends Name<T>> clazz;
    private final NamedParameter namedParameter;
    final Class<T> argClass;
    final Object defaultInstance;

    public boolean isAsSpecificAs(NamedParameterNode<?> n) {
      if (!argClass.equals(n.argClass)) {
        return false;
      }
      if (!name.equals(n.name)) {
        return false;
      }
      if (n.namedParameter == null) {
        return true;
      }
      if (this.namedParameter == null) {
        return false;
      }
      return this.namedParameter.equals(n.namedParameter);
    }

    private NamedParameterNode(Node parent, Class<? extends Name<T>> clazz,
        NamedParameter namedParameter, Class<T> argClass, Object defaultInstance) {
      super(parent, clazz);
      this.clazz = clazz;
      this.namedParameter = namedParameter;
      this.argClass = argClass;
      this.defaultInstance = defaultInstance;
    }

    NamedParameterNode(Node parent, Class<? extends Name<T>> clazz,
        Class<T> argClass) throws BindException {
      super(parent, clazz);
      this.clazz = clazz;
      this.namedParameter = clazz.getAnnotation(NamedParameter.class);
      this.argClass = argClass;
      if (this.namedParameter == null
          || namedParameter.default_value().length() == 0) {
        this.defaultInstance = null;
      } else {
        this.defaultInstance = ReflectionUtilities.parse(this.argClass,
            namedParameter.default_value());
      }
    }

    @Override
    public String toString() {
      String ret = ReflectionUtilities.getSimpleName(argClass);
      if (namedParameter == null) {
        ret = ret + " " + name;
      } else {
        ret = ret + " " + name;
      }
      return ret;
    }

    public Class<T> getArgClass() {
      return argClass;
    }

    public Class<? extends Name<T>> getNameClass() {
      return clazz;
    }

    public Object getDefaultInstance() {
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
    final Class<?> type;
    final Parameter name;

    public String getType() {
      return type.toString();
    }

    public String getName() {
      return name == null ? type.getName() : name.value().getName();
    }

    ConstructorArg(Class<?> argType) {
      this.type = argType;
      this.name = null;
    }

    ConstructorArg(Class<?> type, Parameter name) {
      this.type = type;
      this.name = name;
    }

    @Override
    public String toString() {
      return name == null ? ReflectionUtilities.getSimpleName(type)
          : ReflectionUtilities.getSimpleName(type) + " " + ReflectionUtilities.getSimpleName(name.value());
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

  public class ConstructorDef<T> {
    final ConstructorArg[] args;
    final Constructor<T> constructor;

    public Class<?> getConstructor() {
      return constructor.getDeclaringClass();
    }

    public ConstructorArg[] getArgs() {
      return args;
    }

    @Override
    public String toString() {
      if (args.length == 0) {
        return "()";
      }
      StringBuilder sb = new StringBuilder("(" + args[0]);
      for (int i = 1; i < args.length; i++) {
        sb.append("," + args[i]);
      }
      sb.append(")");
      return sb.toString();
    }

    ConstructorDef(ConstructorArg[] args, Constructor<T> constructor)
        throws BindException {
      this.args = args;
      this.constructor = constructor;
      constructor.setAccessible(true);

      for (int i = 0; i < this.args.length; i++) {
        for (int j = i + 1; j < this.args.length; j++) {
          if (this.args[i].toString().equals(this.args[j].toString())) {
            throw new BindException(
                "Repeated constructor parameter detected.  "
                    + "Cannot inject this constructor.");
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
      if (args.length != def.args.length) {
        return false;
      }
      for (int i = 0; i < args.length; i++) {
        boolean found = false;
        for (int j = 0; j < args.length; j++) {
          if (args[i].getName().equals(args[j].getName())) {
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
      for (int i = 0; i < args.length; i++) {
        boolean found = false;
        for (int j = 0; j < def.args.length; j++) {
          if (args[i].equals(def.args[j])) {
            found = true;
          }
        }
        if (found == false)
          return false;
      }
      return args.length > def.args.length;
    }
  }

}
