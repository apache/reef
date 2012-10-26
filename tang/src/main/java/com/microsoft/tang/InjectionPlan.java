package com.microsoft.tang;

public abstract class InjectionPlan {
  static final InjectionPlan BUILDING = new InjectionPlan() {
    @Override
    public int getNumAlternatives() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "BUILDING INJECTION PLAN";
    }
  };
  public abstract int getNumAlternatives();
  public boolean isFeasible() { return getNumAlternatives() > 0; }
  public boolean isAmbiguous() { return getNumAlternatives() > 1; }
  public boolean isInjectable() { return getNumAlternatives() == 1; }
  
  private static void newline(StringBuffer pretty, int indent) {
    pretty.append('\n');
    for(int j = 0; j < indent * 3; j++) {
      pretty.append(' ');
    }
  }
  public String toPrettyString() {
    String ugly = toString();
    StringBuffer pretty = new StringBuffer();
    int currentIndent = 0;
    for(int i = 0; i < ugly.length(); i++) {
      char c = ugly.charAt(i);
      if(c == '[') {
        currentIndent ++;
        pretty.append(c);
        newline(pretty, currentIndent);
      } else if(c == ']') {
        currentIndent --;
        pretty.append(c);
//        newline(pretty, currentIndent);
      } else if(c == '|') {
        newline(pretty, currentIndent);
        pretty.append(c);
      } else {
        pretty.append(c);
      }
    }
    return pretty.toString();
  }
  
  @Override
  public abstract String toString();
  
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
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(constructor.constructor.getName() + "(");
      if(args.length == 0) {
        sb.append(constructor.constructor.getName() + " has no constructors ");
      } else if(args.length == 1) {
        sb.append(args[0]);
      } else {
        sb.append("[" + args[1]);
        for(int i = 1; i < args.length; i++) {
          sb.append(" | " + args[i]);
        }
        sb.append("]");
      }
      sb.append(")");
      return sb.toString();
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

    @Override
    public String toString() {
      return name + " " + instance;
    }
  }
  final public static class InfeasibleInjectionPlan extends InjectionPlan {
    final String name;
    public InfeasibleInjectionPlan(String name) {
      this.name = name;
    }
    @Override
    public int getNumAlternatives() {
      return 0;
    }

    @Override
    public String toString() {
      return "(no injectors for " + name + ")";
    }
  }

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
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("[ ");
      if(this.alternatives.length == 0) {
        sb.append("no impls");
      } else {
        sb.append(alternatives[0]);
      }
      for(int i = 1; i < alternatives.length; i++) {
        sb.append(" | " + alternatives[i]);
      }
      sb.append(" ]");
      return sb.toString();
    }
  }
}
