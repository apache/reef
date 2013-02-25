package com.microsoft.tang.implementation;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;

final public class Constructor<T> extends InjectionPlan<T> {
    final ConstructorDef<T> constructor;
    final InjectionPlan<?>[] args;
    final int numAlternatives;
    final boolean isAmbiguous;
    final boolean isInjectable;
    public InjectionPlan<?>[] getArgs() {
    	return args;
    }
    public ConstructorDef<T> getConstructorDef() {
    	return constructor;
    }
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
        if (a.isAmbiguous())
          isAmbiguous = true;
        if (!a.isInjectable())
          isInjectable = false;
      }
      this.numAlternatives = numAlternatives;
      this.isAmbiguous = isAmbiguous;
      this.isInjectable = isInjectable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ClassNode<T> getNode() {
      return (ClassNode<T>) node;
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
      StringBuilder sb = new StringBuilder("new " + getNode().getName() + "(");
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