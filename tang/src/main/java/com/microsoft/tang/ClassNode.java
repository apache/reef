package com.microsoft.tang;

import com.microsoft.tang.exceptions.BindException;


public interface ClassNode<T> extends Node {

  public boolean getIsPrefixTarget();

  public ConstructorDef<T>[] getInjectableConstructors();

  @Deprecated // should take an array of nodes instead.
  public ConstructorDef<T> createConstructorDef(Class<?>... args) throws BindException;
}