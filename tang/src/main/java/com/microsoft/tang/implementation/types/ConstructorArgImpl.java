package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.ConstructorArg;

public class ConstructorArgImpl implements ConstructorArg {
  private final String type;
  private final String name;

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

  public ConstructorArgImpl(String type, String namedParameterName) {
    this.type = type;
    this.name = namedParameterName;
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
}