package com.microsoft.tang;

import com.microsoft.tang.exceptions.BindException;

public interface ClassNode<T> extends Node {
  public boolean getIsPrefixTarget();

  public ConstructorDef<T>[] getInjectableConstructors();

  public ConstructorDef<T> getConstructorDef(ClassNode<?>... args)
      throws BindException;

  public ConstructorDef<T>[] getAllConstructors();

  public boolean isInjectionCandidate();
}