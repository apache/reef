package com.microsoft.tang.implementation;

import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;

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
  final public static class DelegatedImpl<T> extends InjectionPlan<T> {
    final InjectionPlan<? extends T> impl;
    final Node cn;
    public DelegatedImpl(Node cn, InjectionPlan<? extends T> impl) {
      this.cn = cn;
      this.impl = impl;
    }
    public Node getNode() {
      return cn;
    }
    @Override
    public int getNumAlternatives() {
      return impl.getNumAlternatives();
    }
    @Override
    public boolean isAmbiguous() {
      return impl.isAmbiguous();
    }
    @Override
    public boolean isInjectable() {
      return impl.isInjectable();
    }
    @Override
    public String toString() {
      return "(" + cn + " provided by " + impl.toString() + ")";
    }
  }
  
  final public static class Constructor<T> extends InjectionPlan<T> {
    final TypeHierarchy.ConstructorDef<T> constructor;
    final InjectionPlan<?>[] args;
    final int numAlternatives;
    final boolean isAmbiguous;
    final boolean isInjectable;
    final ClassNode<T> cn;
    
    public Constructor(ClassNode<T> cn, TypeHierarchy.ConstructorDef<T> constructor,
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
      this.cn = cn;
    }

    public ClassNode<T> getNode() { return cn; }
    
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
      StringBuilder sb = new StringBuilder("new " + constructor.getConstructor().getDeclaringClass().getSimpleName()
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

  final public static class AmbiguousInjectionPlan<T> extends InjectionPlan<T> {
    final InjectionPlan<? extends T>[] alternatives;
    final int numAlternatives;
    final int selectedIndex;
    public AmbiguousInjectionPlan(int selectedIndex, InjectionPlan<? extends T>[] alternatives) {
      this.alternatives = alternatives;
      if(selectedIndex < 0 || selectedIndex >= alternatives.length) {
        throw new ArrayIndexOutOfBoundsException();
      }
      this.selectedIndex = selectedIndex;
      this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
    }
    public AmbiguousInjectionPlan(InjectionPlan<? extends T>[] alternatives) {
      this.alternatives = alternatives;
      this.selectedIndex = -1;
      int numAlternatives = 0;
      for (InjectionPlan<? extends T> a : alternatives) {
        numAlternatives += a.getNumAlternatives();
      }
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
      if(selectedIndex == -1) {
        return true;
      }
      return alternatives[selectedIndex].isAmbiguous();
    }

    @Override
    public boolean isInjectable() {
      return false;
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
