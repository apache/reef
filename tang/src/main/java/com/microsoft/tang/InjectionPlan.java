package com.microsoft.tang;

final public class InjectionPlan {
  final public class Constructor {
    final TypeHierarchy.ConstructorDef constructor;
    final InjectionPlan[] args;
    public Constructor(TypeHierarchy.ConstructorDef constructor, InjectionPlan[] args) {
      this.constructor = constructor;
      this.args = args;
    }
  }
  final public class Instance{
    final TypeHierarchy.NamedParameterNode value;
    public Instance(TypeHierarchy.NamedParameterNode value) {
      this.value = value;
    }
  }
  final public class AmbiguousInjectionPlan {
    final InjectionPlan[] alternatives;
    public AmbiguousInjectionPlan(InjectionPlan[] alternatives) {
      this.alternatives = alternatives;
    }
  }
}
