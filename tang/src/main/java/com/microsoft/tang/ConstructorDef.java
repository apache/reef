package com.microsoft.tang;


public interface ConstructorDef<T> extends Comparable<ConstructorDef<?>> {

  public ConstructorArg[] getArgs();

  public boolean isMoreSpecificThan(ConstructorDef<?> def);

}