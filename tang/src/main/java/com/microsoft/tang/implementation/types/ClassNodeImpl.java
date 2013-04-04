package com.microsoft.tang.implementation.types;

import java.util.Set;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicSet;

public class ClassNodeImpl<T> extends AbstractNode implements ClassNode<T> {
  private final boolean injectable;
  private final boolean unit;
  private final boolean externalConstructor;
  private final ConstructorDef<T>[] injectableConstructors;
  private final ConstructorDef<T>[] allConstructors;
  private final MonotonicSet<ClassNode<? extends T>> knownImpls;
  public ClassNodeImpl(Node parent, String simpleName, String fullName,
      boolean unit, boolean injectable, boolean externalConstructor,
      ConstructorDef<T>[] injectableConstructors,
      ConstructorDef<T>[] allConstructors) {
    super(parent, simpleName, fullName);
    this.unit = unit;
    this.injectable = injectable;
    this.externalConstructor = externalConstructor;
    this.injectableConstructors = injectableConstructors;
    this.allConstructors = allConstructors;
    this.knownImpls = new MonotonicSet<>();
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
  public boolean isInjectionCandidate() {
    return injectable;
  }
  @Override
  public boolean isExternalConstructor() {
    return externalConstructor;
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
      throw new BindException("Cannot @Inject non-static member/local class: "
          + getFullName());
    }
    for (ConstructorDef<T> c : getAllConstructors()) {
      if (c.takesParameters(paramTypes)) {
        return c;
      }
    }
    throw new BindException("Could not find requested constructor for class "
        + getFullName());
  }

  @Override
  public void putImpl(ClassNode<? extends T> impl) {
    knownImpls.add(impl);
  }

  @Override
  public Set<ClassNode<? extends T>> getKnownImplementations() {
   return new MonotonicSet<>(knownImpls);
  }

  @Override
  public boolean isUnit() {
    return unit;
  }
}