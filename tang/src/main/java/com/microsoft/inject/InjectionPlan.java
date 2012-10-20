package com.microsoft.inject;

public class InjectionPlan {
  public class Constructor {
    TypeHierarchy.ConstructorDef constructor;
    InjectionPlan[] args;
  }
  public class Instance{
    TypeHierarchy.NamedParameterNode value;
  }
}
