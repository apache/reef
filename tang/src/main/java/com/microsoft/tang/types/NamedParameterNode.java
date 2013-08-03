package com.microsoft.tang.types;

public interface NamedParameterNode<T> extends Node {

  public String getDocumentation();

  public String getShortName();

  public String getDefaultInstanceAsString();

  public String getSimpleArgName();

  public String getFullArgName();
  
  public boolean isSet();
}