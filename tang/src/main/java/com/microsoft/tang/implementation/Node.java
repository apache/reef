package com.microsoft.tang.implementation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public abstract class Node implements Comparable<Node> {
  @SuppressWarnings("unchecked")
  public static class ClassNode<T> extends Node {
    private final Class<T> clazz;
    private final boolean isPrefixTarget;
    private final ConstructorDef<T>[] injectableConstructors;
  
    Class<T> getClazz() {
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
  
    ConstructorDef<T> createConstructorDef(Class<?>... paramTypes)
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
  
    ClassNode(Node parent, Class<T> clazz, boolean isPrefixTarget) throws BindException {
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

  public static class ConstructorArg {
    private final Class<?> type;
    private final Parameter name;
  
    public String getName() {
      return name == null ? type.getName() : name.value().getName();
    }
    public Parameter getNamedParameter() {
      return name;
    }
    @Deprecated
    public Class<?> getType() {
      return type;
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

  public static class ConstructorDef<T> implements Comparable<ConstructorDef<?>> {
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

  public static class NamedParameterNode<T> extends Node {
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
  
    Class<T> getArgClass() {
      return argClass;
    }
  
    Class<? extends Name<T>> getNameClass() {
      return nameClass;
    }
  
    T getDefaultInstance() {
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

  public static class NamespaceNode<T> extends Node {
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

  public static class PackageNode extends Node {
    PackageNode(Node parent, String name) {
      super(parent, name);
    }
  }

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