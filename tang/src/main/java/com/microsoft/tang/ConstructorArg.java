package com.microsoft.tang;

import com.microsoft.tang.annotations.Parameter;

public interface ConstructorArg {

  public String getName();

  public Parameter getNamedParameter();

  public String getType();

}