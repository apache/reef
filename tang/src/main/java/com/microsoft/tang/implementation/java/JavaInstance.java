package com.microsoft.tang.implementation.java;

import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.types.Node;

final public class JavaInstance<T> extends InjectionPlan<T> {
    final T instance;

    public JavaInstance(Node name, T instance) {
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
      return instance != null;
    }

    @Override
    public String toCantInjectString(int indent) {
      if(instance == null) {
        return this.getClass() + ": bound to null instance";
      } else {
        throw new IllegalArgumentException("toCantInjectString called on injectable instance!");
      }
    }
  }