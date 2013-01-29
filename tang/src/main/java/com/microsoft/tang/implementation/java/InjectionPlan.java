package com.microsoft.tang.implementation.java;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.Node;

public abstract class InjectionPlan<T> {
  final Node node;
  static final InjectionPlan<?> BUILDING = new InjectionPlan<Object>(null) {
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
  public InjectionPlan(Node node) {
    this.node = node;
  }
  public Node getNode() {
    return node;
  }
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
  
  final public static class Constructor<T> extends InjectionPlan<T> {
    final ConstructorDef<T> constructor;
    final InjectionPlan<?>[] args;
    final int numAlternatives;
    final boolean isAmbiguous;
    final boolean isInjectable;
    
    public Constructor(ClassNode<T> cn, ConstructorDef<T> constructor,
        InjectionPlan<?>[] args) {
      super(cn);
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

    @SuppressWarnings("unchecked")
    @Override
    public ClassNode<T> getNode() { return (ClassNode<T>) node; }
    
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
      StringBuilder sb = new StringBuilder("new " + getNode().getName()
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
    final T instance;

    public Instance(Node name, T instance) {
      super(name);
      this.instance = instance;
    }

    @Override
    public int getNumAlternatives() {
      return instance == null ? 0 : 1;
    }

    @Override
    public String toString() {
      return getNode() + " = " + instance;
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

  final public static class Subplan<T> extends InjectionPlan<T> {
    final InjectionPlan<? extends T>[] alternatives;
    final int numAlternatives;
    final int selectedIndex;
    public Subplan(Node node, int selectedIndex, @SuppressWarnings("unchecked") InjectionPlan<? extends T>... alternatives) {
      super(node);
      this.alternatives = alternatives;
      if(selectedIndex < 0 || selectedIndex >= alternatives.length) {
        throw new ArrayIndexOutOfBoundsException();
      }
      this.selectedIndex = selectedIndex;
      this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
    }
    public Subplan(Node node, @SuppressWarnings("unchecked") InjectionPlan<? extends T>... alternatives) {
      super(node);
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
      if(selectedIndex == -1) {
        return false;
      } else {
        return alternatives[selectedIndex].isInjectable();
      }
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
    public InjectionPlan<? extends T> getDelegatedPlan() {
      if(selectedIndex == -1) {
        throw new IllegalStateException();
      } else {
        return alternatives[selectedIndex];
      }
    }
  }
}
