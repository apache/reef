package com.microsoft.tang.types;

public interface ConstructorArg {

  public String getName();

  public String getType();

  public boolean isInjectionFuture();
  
  public String getNamedParameterName();  
}