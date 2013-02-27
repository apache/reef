package com.microsoft.tang.types;

public interface NamedParameterNode<T> extends Node {

  public String getDocumentation();

  public String getShortName();

  public String getDefaultInstanceAsString();

  String getSimpleArgName();

  String getFullArgName();
}