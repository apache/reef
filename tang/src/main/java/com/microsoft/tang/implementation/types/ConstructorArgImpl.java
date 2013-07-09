package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.ConstructorArg;

public class ConstructorArgImpl implements ConstructorArg {
  private final String type;
  private final String name;
  private final boolean isInjectionFuture;
  
  @Override
  public String getName() {
    return name == null ? type : name;
  }

  @Override
  public String getNamedParameterName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  public ConstructorArgImpl(String type, String namedParameterName, boolean isInjectionFuture) {
    this.type = type;
    this.name = namedParameterName;
    this.isInjectionFuture = isInjectionFuture;
  }

  @Override
  public String toString() {
    return name == null ? type : type + " " + name;
  }

  @Override
  public boolean equals(Object o) {
    ConstructorArgImpl arg = (ConstructorArgImpl) o;
    if (!type.equals(arg.type)) {
      return false;
    }
    if (name == null && arg.name == null) {
      return true;
    }
    if (name == null && arg.name != null) {
      return false;
    }
    if (name != null && arg.name == null) {
      return false;
    }
    return name.equals(arg.name);

  }

  @Override
  public boolean isInjectionFuture() {
    return isInjectionFuture;
  }
}