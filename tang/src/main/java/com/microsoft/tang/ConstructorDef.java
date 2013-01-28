package com.microsoft.tang;

import java.lang.reflect.Constructor;


public interface ConstructorDef<T> extends Comparable<ConstructorDef<?>> {

  public ConstructorArg[] getArgs();

  public boolean isMoreSpecificThan(ConstructorDef<?> def);
  @Deprecated
  public Constructor<T> getConstructor();

  public boolean takesParameters(Class<?>[] paramTypes);

}