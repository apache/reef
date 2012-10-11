package com.microsoft.inject;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

@ConfigurationMetadata(name = "ns.impl", params = { @NamedParameter(value = "foo", doc = "a second string", default_value = "default") })
public class Namespace {

  final static String regexp = "[\\.\\$]";

  @Inject
  public Namespace() {
  }

  @Inject
  public Namespace(String two) {

  }

  @Inject
  public Namespace(@Named("foo") String exportedNamespace, String two) {

  }

  class Node {
    private final String name;

    Map<String, Node> children = new HashMap<String, Node>();

    Node(String name) {
      this.name = name;
    }

    public boolean contains(String key) {
      return children.containsKey(key);
    }

    public Node get(String key) {
      return children.get(key);
    }

    private void put(Node n) {
      Node old = children.get(n.name);
      if (old != null && !old.getClass().equals(Node.class)) {
        throw new IllegalStateException(
            "Attempt to add node that already exists!");
      }
      if (old != null) {
        if (!old.children.isEmpty()) {
          if (n instanceof ClassNode) {
            n.children.putAll(old.children);
          } else {
            throw new IllegalStateException(
                "Somehow ended up with leaf node that has children");
          }
        }
      }
      children.put(n.name, n);
    }

    public void addNamedParameter(NamedParameter name) {
      put(new NamedParameterNode(name));

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
      return name + " [" + this.getClass().getSimpleName() + "]";
    }
  }

  class ClassNode extends Node {
    final Class<?> clazz;
    final boolean isPrefixTarget;
    final ConstructorDef[] injectableConstructors;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString() + ": ");
      for (ConstructorDef c : injectableConstructors) {
        sb.append(c.toString() + ", ");
      }
      return sb.toString();
    }

    public ClassNode(Class<?> clazz, boolean isPrefixTarget) {
      super(clazz.getSimpleName());
      if (clazz.isLocalClass() || clazz.isMemberClass()) {
        if (!Modifier.isStatic(clazz.getModifiers())) {
          throw new IllegalArgumentException(
              "Cannot @Inject non-static member/local class: " + clazz);
        }
      }
      this.clazz = clazz;
      this.isPrefixTarget = isPrefixTarget;

      boolean injectAllConstructors = (clazz.getAnnotation(Inject.class) != null);
      Constructor<?>[] constructors = clazz.getConstructors();
      List<ConstructorDef> injectableConstructors = new ArrayList<ConstructorDef>();

      for (int k = 0; k < constructors.length; k++) {

        if (injectAllConstructors
            || null != constructors[k].getAnnotation(Inject.class)) {

          // go through the constructor arguments.
          if (constructors[k].isSynthetic()) {
            throw new IllegalArgumentException(
                "Attempt to make synthetic constructor injectable.");
          }

          // ConstructorDef's constructor checks for duplicate
          // parameters
          // The injectableConstructors set checks for ambiguous
          // constructors.
          Class<?>[] paramTypes = constructors[k].getParameterTypes();
          Annotation[][] paramAnnotations = constructors[k]
              .getParameterAnnotations();
          if (paramTypes.length != paramAnnotations.length) {
            throw new IllegalStateException();
          }
          ConstructorArg[] args = new ConstructorArg[paramTypes.length];
          for (int i = 0; i < paramTypes.length; i++) {
            // if there is an appropriate annotation, use that.
            Named named = null;
            for (int j = 0; j < paramAnnotations[i].length; j++) {
              Annotation annotation = paramAnnotations[i][j];
              if (annotation instanceof Named) {
                named = (Named) annotation;
              }
            }
            args[i] = new ConstructorArg(paramTypes[i], named);
            children.put(args[i].getName(), new NamedParameterNode(args[i].getName(), args[i].type));
          }
          ConstructorDef def = new ConstructorDef(args, constructors[k]);
          if (injectableConstructors.contains(def)) {
            throw new IllegalStateException(
                "Ambiguous constructors detected in class " + clazz + ": "
                    + def + " differs from some other " + " constructor only "
                    + "by parameter order.");
          } else {
            injectableConstructors.add(def);
          }
        }
      }
      this.injectableConstructors = injectableConstructors
          .toArray(new ConstructorDef[0]);
    }
  }

  class ConfigurationPrefixNode extends Node {
    final Node target;

    public ConfigurationPrefixNode(String name, ClassNode target) {
      super(name);
      children = null;
      if (!target.isPrefixTarget) {
        throw new IllegalStateException();
      }
      this.target = target;
    }

    @Override
    public String toString() {
      return super.toString() + " -> " + target.toString();
    }
  }

  class NamedParameterNode extends Node {
    private final NamedParameter namedParameter;
    final Class<?> argClass;

    NamedParameterNode(NamedParameter n) {
      super(n.value());
      children = null;
      this.namedParameter = n;
      try {
        this.argClass = Class.forName(n.type());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Named parameter " + n.toString()
            + " takes unknown class as argument: " + n.type() + ".");
      }
    }
    NamedParameterNode(String name, Class<?> argClass) {
      super(name);
      this.argClass = argClass;
      this.namedParameter = null;
    }

    @Override
    public String toString() {
      return argClass.getSimpleName()
          + " "
          + super.toString()
          + (namedParameter.default_value() != null ? (" default=" + namedParameter
              .default_value()) : "")
          + (namedParameter.doc() != null ? (" Documentation: " + namedParameter
              .doc()) : "");
    }
  }

  class ConstructorArg {
    final Class<?> type;
    final Named name;

    String getName() {
      return name == null ? type.getName() : name.value();
    }
    String getFullyQualifiedName(Class<?> targetClass) {
      String name = getName();
      if (!name.contains(".")) {
        name = targetClass.getName() + "." + name;
      }
      return name;
    }
    ConstructorArg(Class<?> argType) {
      this.type = argType;
      this.name = null;
    }

    ConstructorArg(Class<?> type, Named name) {
      this.type = type;
      this.name = name;
      if (name != null && name.value().equals("")) {
        throw new IllegalArgumentException(
            "Named parameters with the empty name (\"\") aren't allowed!");
      }
    }

    @Override
    public String toString() {
      return name == null ? type.getSimpleName()
          : (type.getSimpleName() + " " + name.value());
    }
  }

  class ConstructorDef {
    final ConstructorArg[] args;
    final Constructor<?> constructor;
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

    ConstructorDef(ConstructorArg[] args, Constructor constructor) {
      this.args = args;
      this.constructor = constructor;
      for (int i = 0; i < this.args.length; i++) {
        for (int j = i + 1; j < this.args.length; j++) {
          if (this.args[i].toString().equals(this.args[j].toString())) {
            throw new IllegalArgumentException(
                "Repeated constructor parameter detected.  "
                    + "Cannot inject this constructor.");
          }
        }
      }
    }

    /**
     * Check to see if two constructors take indistinguishable arguments. If so
     * (and they are in the same class), then this would lead to ambiguous
     * injection targets, and we want to fail fast.
     * 
     * TODO could be faster. Currently O(n^2) in number of parameters.
     * 
     * @param def
     * @return
     */
    boolean equalsIgnoreOrder(ConstructorDef def) {
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
      return equalsIgnoreOrder((ConstructorDef) o);
    }
  }

  final Node namespace = new Node("");

  private ConfigurationPrefixNode buildPathToNode(ConfigurationMetadata conf,
      ClassNode classNode) {
    String[] path = conf.name().split(regexp);
    Node root = namespace;
    for (int i = 0; i < path.length - 1; i++) {
      if (!root.contains(path[i])) {
        Node newRoot = new Node(path[i]);
        root.put(newRoot);
        root = newRoot;
      } else {
        root = root.get(path[i]);
      }
    }
    ConfigurationPrefixNode ret = new ConfigurationPrefixNode(
        path[path.length - 1], classNode);
    root.put(ret);
    return ret;

  }

  private ClassNode buildPathToNode(Class<?> clazz, boolean isPrefixTarget) {
    String[] path = clazz.getName().split(regexp);
    Node root = namespace;
    ClassNode ret = null;
    for (int i = 0; i < path.length - 1; i++) {
      if (!root.contains(path[i])) {
        Node newRoot = new Node(path[i]);
        root.put(newRoot);
        root = newRoot;
      } else {
        root = root.get(path[i]);
      }
    }
    ret = new ClassNode(clazz, isPrefixTarget);
    root.put(ret);
    return ret;
  }

  Node getNode(Class<?> clazz) {
    return getNode(clazz.getName());
  }

  Node getNode(String name) {
    String[] path = name.split(regexp);
    return getNode(path, path.length);
  }

  private Node getNode(String[] path, int depth) {
    Node root = namespace;
    for (int i = 0; i < depth; i++) {
      if (root instanceof ConfigurationPrefixNode) {
        root = ((ConfigurationPrefixNode) root).target;
      }
      root = root.get(path[i]);
      if (root == null)
        return null;
    }
    return root;
  }

  public void registerClass(Class<?> c) {
    if (c.isArray()) {
      throw new UnsupportedOperationException("Can't register array types");
    }
    // if (c.isPrimitive()) {
    // throw new UnsupportedOperationException(
    // "Can't register primitive types");
    // }

    ConfigurationMetadata confAnnotation = c
        .getAnnotation(ConfigurationMetadata.class);
    ClassNode n;
    if (confAnnotation == null || confAnnotation.name() == null) {
      n = buildPathToNode(c, false);
    } else {
      n = buildPathToNode(c, true);
      buildPathToNode(confAnnotation, n);
    }

    if (confAnnotation != null) {
      for (NamedParameter name : confAnnotation.params()) {
        n.addNamedParameter(name);
      }
    }
  }

  public String exportNamespace() {
    return namespace.toIndentedString(0);
  }

  public void findUnresolvedClasses(Node root, Set<Class<?>> unresolved) {
    if (root instanceof ClassNode) {
      ClassNode cls = (ClassNode) root;
      for (ConstructorDef def : cls.injectableConstructors) {
        for (ConstructorArg arg : def.args) {
          if (getNode(arg.type) == null) {
            unresolved.add(arg.type);
          }
        }
      }
      Class<?> zuper = cls.clazz.getSuperclass();
      if (zuper != null && getNode(zuper) == null)
        unresolved.add(zuper);
      Class<?>[] interfaces = cls.clazz.getInterfaces();
      for (Class<?> i : interfaces) {
        if (getNode(i) == null)
          unresolved.add(i);
      }
    }
    if (root instanceof NamedParameterNode) {
      NamedParameterNode np = (NamedParameterNode) (root);
      if (getNode(np.argClass) == null) {
        unresolved.add(np.argClass);
      }
    }
    if (root.children != null) {
      for (Node n : root.children.values()) {
        findUnresolvedClasses(n, unresolved);
      }
    }
  }

  public Class<?>[] findUnresolvedClasses() {
    Set<Class<?>> unresolved = new HashSet<Class<?>>();
    findUnresolvedClasses(namespace, unresolved);
    return unresolved.toArray(new Class<?>[0]);
  }

  public void resolveAllClasses() {
    for (Class<?>[] classes = findUnresolvedClasses(); classes.length > 0; classes = findUnresolvedClasses()) {
      for (Class<?> c : classes) {
        registerClass(c);
      }
    }
  }

  // TODO: Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.

  public static void main(String[] args) throws Exception {
    Namespace ns = new Namespace();
    for (String s : args) {
      ns.registerClass(Class.forName(s));
    }
    for (Class<?>[] classes = ns.findUnresolvedClasses(); classes.length > 0; classes = ns
        .findUnresolvedClasses()) {
      System.out.println("Found unresolved classes.  Loading them.");
      for (Class<?> c : classes) {
        System.out.println("  " + c.getName());
        ns.registerClass(c);
      }
      System.out.println("Done.");
    }
    System.out.print(ns.exportNamespace());
  }
}
