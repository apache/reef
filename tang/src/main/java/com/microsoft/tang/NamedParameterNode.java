package com.microsoft.tang;


public interface NamedParameterNode<T> extends Node {

  public String getDocumentation();

  /*
   * public NamedParameter getNamedParameter() { return namedParameter; }
   */
  public String getShortName();

}