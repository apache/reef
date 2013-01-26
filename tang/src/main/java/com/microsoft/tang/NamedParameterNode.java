package com.microsoft.tang;

import com.microsoft.tang.annotations.Name;



public interface NamedParameterNode<T> extends Node {

  public String getDocumentation();

  /*
   * public NamedParameter getNamedParameter() { return namedParameter; }
   */
  public String getShortName();

  @Deprecated
  public Class<? extends Name<T>> getNameClass();

  @Deprecated
  public Class<T> getArgClass();
  
  public String getDefaultInstanceAsString();

  @Deprecated
  public T getDefaultInstance();
}