package com.microsoft.tang.implementation;

public abstract class InjectionPlan<T> {
  static final InjectionPlan<?> BUILDING = new InjectionPlan<Object>() {
    @Override
    public int getNumAlternatives() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "BUILDING INJECTION PLAN";
    }

    @Override
    public boolean isAmbiguous() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInjectable() {
      throw new UnsupportedOperationException();
    }
  };

  public abstract int getNumAlternatives();

  public boolean isFeasible() {
    return getNumAlternatives() > 0;
  }

  abstract public boolean isAmbiguous();
  abstract public boolean isInjectable();
  
  private static void newline(StringBuffer pretty, int indent) {
    pretty.append('\n');
    for (int j = 0; j < indent * 3; j++) {
      pretty.append(' ');
    }
  }

  public String toPrettyString() {
    String ugly = toString();
    StringBuffer pretty = new StringBuffer();
    int currentIndent = 0;
    for (int i = 0; i < ugly.length(); i++) {
      char c = ugly.charAt(i);
      if (c == '[') {
        currentIndent++;
        pretty.append(c);
        newline(pretty, currentIndent);
      } else if (c == ']') {
        currentIndent--;
        pretty.append(c);
        // newline(pretty, currentIndent);
      } else if (c == '|') {
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

  final public static class Constructor<T> extends InjectionPlan<T> {
    final TypeHierarchy.ConstructorDef<T> constructor;
    final InjectionPlan<?>[] args;
    final int numAlternatives;
    final boolean isAmbiguous;
    final boolean isInjectable;
    
    public Constructor(TypeHierarchy.ConstructorDef<T> constructor,
        InjectionPlan<?>[] args) {
      this.constructor = constructor;
      this.args = args;
      int numAlternatives = 1;
      boolean isAmbiguous = false;
      boolean isInjectable = true;
      for (InjectionPlan<?> a : args) {
        numAlternatives *= a.getNumAlternatives();
        if(a.isAmbiguous()) isAmbiguous = true;
        if(!a.isInjectable()) isInjectable = false;
      }
      this.numAlternatives = numAlternatives;
      this.isAmbiguous = isAmbiguous;
      this.isInjectable = isInjectable;
    }

    @Override
    public int getNumAlternatives() {
      return numAlternatives;
    }

    @Override
    public boolean isAmbiguous() {
      return isAmbiguous;
    }

    @Override
    public boolean isInjectable() {
      return isInjectable;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("new " + constructor.constructor.getDeclaringClass().getSimpleName()
          + "(");
      if (args.length == 0) {
      } else if (args.length == 1) {
        sb.append(args[0]);
      } else {
        sb.append(args[0]);
        for (int i = 1; i < args.length; i++) {
          sb.append(", " + args[i]);
        }
      }
      sb.append(")");
      return sb.toString();
    }
  }

  final public static class Instance<T> extends InjectionPlan<T> {
    final TypeHierarchy.Node name;
    final T instance;

    public Instance(TypeHierarchy.Node name, T instance) {
      this.name = name;
      this.instance = instance;
    }

    @Override
    public int getNumAlternatives() {
      return instance == null ? 0 : 1;
    }

    @Override
    public String toString() {
      return name + " = " + instance;
    }

    @Override
    public boolean isAmbiguous() {
      return false;
    }

    @Override
    public boolean isInjectable() {
      return true;
    }
  }

  final public static class InfeasibleInjectionPlan<T> extends InjectionPlan<T> {
    final String name;

    public InfeasibleInjectionPlan(String name) {
      this.name = name;
    }

    @Override
    public int getNumAlternatives() {
      return 0;
    }
    // XXX better error message here!
    @Override
    public String toString() {
      return "(no injectors for " + name + ")";
    }

    @Override
    public boolean isAmbiguous() {
      return false;
    }

    @Override
    public boolean isInjectable() {
      return false;
    }
  }

  final public static class AmbiguousInjectionPlan<T> extends InjectionPlan<T> {
    final InjectionPlan<? extends T>[] alternatives;
    final int numAlternatives;
    final boolean isInjectable;
    final boolean isAmbiguous;
    
    public AmbiguousInjectionPlan(InjectionPlan<? extends T>[] alternatives) {
      this.alternatives = alternatives;
      int numAlternatives = 0;
      boolean isInjectable = true;
      this.isAmbiguous = true;
      for (InjectionPlan<? extends T> a : alternatives) {
        numAlternatives += a.getNumAlternatives();
        if(!a.isInjectable()) isInjectable = false;
      }
      this.isInjectable = isInjectable;
      this.numAlternatives = numAlternatives;
    }

    @Override
    public int getNumAlternatives() {
      return this.numAlternatives;
    }
    /**
     * Even if there is only one sub-plan, it was registered as a default
     * plan, and is therefore ambiguous.
     */
    @Override
    public boolean isAmbiguous() {
      return true;
    }

    @Override
    public boolean isInjectable() {
      return isInjectable;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("[");
      if (this.alternatives.length == 0) {
        sb.append("no impls");
      } else {
        sb.append(alternatives[0]);
      }
      for (int i = 1; i < alternatives.length; i++) {
        sb.append(" | " + alternatives[i]);
      }
      sb.append("]");
      return sb.toString();
    }

  }
}
