package com.microsoft.tang;

public interface NamedParameterNode<T> extends Node {

  public String getDocumentation();

  public String getShortName();
  public String getFullName();
  
  public String getDefaultInstanceAsString();

  @Deprecated
  public T getDefaultInstance();

  String getSimpleArgName();

  String getFullArgName();
}