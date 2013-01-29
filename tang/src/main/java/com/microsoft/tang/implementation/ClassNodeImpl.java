package com.microsoft.tang.implementation;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.Node;
import com.microsoft.tang.exceptions.BindException;

public class ClassNodeImpl<T> extends AbstractNode implements
    ClassNode<T> {
  private final boolean injectable;
  // TODO: Would like to get rid of fullName in JavaClassNode, but getting "."
  // vs "$" right in classnames is tricky.
  private final String fullName;
  private final boolean isPrefixTarget;
  private final ConstructorDef<T>[] injectableConstructors;
  private final ConstructorDef<T>[] allConstructors;

  public ClassNodeImpl(Node parent, String simpleName, String fullName,
      boolean injectable, boolean isPrefixTarget,
      ConstructorDef<T>[] injectableConstructors,
      ConstructorDef<T>[] allConstructors) {
    super(parent, simpleName);
    this.fullName = fullName;
    this.injectable = injectable;
    this.isPrefixTarget = isPrefixTarget;
    this.injectableConstructors = injectableConstructors;
    this.allConstructors = allConstructors;
  }

  @Override
  public boolean getIsPrefixTarget() {
    return isPrefixTarget;
  }

  @Override
  public ConstructorDef<T>[] getInjectableConstructors() {
    return injectableConstructors;
  }
  @Override
  public ConstructorDef<T>[] getAllConstructors() {
    return allConstructors;
  }

  @Override
  public String getFullName() {
    return fullName;
  }

  @Override
  public boolean isInjectionCandidate() {
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

  // TODO: Instead of keeping clazz around, we need to remember all of the
  // constructors for
  // the class, and have a method that forces the "injectable" bit on the
  // constructors to be true.
  public ConstructorDef<T> getConstructorDef(ClassNode<?>... paramTypes)
      throws BindException {
    if (!isInjectionCandidate()) {
      throw new BindException(
          "Cannot @Inject non-static member/local class: " + getFullName());
    }
    for (ConstructorDef<T> c : getAllConstructors()) {
      if (c.takesParameters(paramTypes)) {
        return c;
      }
    }
    throw new BindException("Could not find requested constructor for class "
        + getFullName());
  }


}