package com.microsoft.tang;


public interface ClassNode<T> extends Node {

  public boolean getIsPrefixTarget();

  public ConstructorDef<T>[] getInjectableConstructors();

}