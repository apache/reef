package com.microsoft.tang;

public interface ConstructorDef<T> extends Comparable<ConstructorDef<?>> {
  public String getClassName();

  public ConstructorArg[] getArgs();

  public boolean isMoreSpecificThan(ConstructorDef<?> def);

  public boolean takesParameters(ClassNode<?>[] paramTypes);
}