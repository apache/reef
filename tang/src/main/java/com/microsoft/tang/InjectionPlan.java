package com.microsoft.tang;

public abstract class InjectionPlan {
  public abstract int getNumAlternatives();
  public boolean isFeasible() { return getNumAlternatives() > 0; }
  public boolean isAmbiguous() { return getNumAlternatives() > 1; }
  public boolean canInject() { return getNumAlternatives() == 1; }
  
  final public static class Constructor extends InjectionPlan {
    final TypeHierarchy.ConstructorDef constructor;
    final InjectionPlan[] args;
    final int numAlternatives;
    public Constructor(TypeHierarchy.ConstructorDef constructor, InjectionPlan[] args) {
      this.constructor = constructor;
      this.args = args;
      int numAlternatives = 1;
      for(InjectionPlan a : args) {
        numAlternatives *= a.getNumAlternatives();
      }
      this.numAlternatives = numAlternatives;
    }
    @Override
    public int getNumAlternatives() {
      return numAlternatives;
    }
  }
  final public static class Instance extends InjectionPlan {
    final TypeHierarchy.Node name;
    final Object instance;
    public Instance(TypeHierarchy.Node name, Object instance) {
      this.name = name;
      this.instance = instance;
    }
    @Override
    public int getNumAlternatives() {
      return instance == null ? 0 : 1;
    }
  }
  final public static class InfeasibleInjectionPlan extends InjectionPlan {
    @Override
    public int getNumAlternatives() {
      return 0;
    } }

  final public static class AmbiguousInjectionPlan extends InjectionPlan {
    final InjectionPlan[] alternatives;
    final int numAlternatives;
    public AmbiguousInjectionPlan(InjectionPlan[] alternatives) {
      this.alternatives = alternatives;
      int numAlternatives = 0;
      for(InjectionPlan a : alternatives) {
        numAlternatives += a.getNumAlternatives();
      }
      this.numAlternatives = numAlternatives;
    }
    @Override
    public int getNumAlternatives() {
      return this.numAlternatives;
    }
  }
}
