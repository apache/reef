package com.microsoft.tang.implementation.java;

import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

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
    protected String toAmbiguousInjectString() {
      throw new IllegalArgumentException("toAmbiguousInjectString called on JavaInstance!" + this.toString());
    }

    @Override
    protected String toInfeasibleInjectString() {
      throw new IllegalArgumentException("toAmbiguousInjectString called on JavaInstance!" + this.toString());
    }

    @Override
    protected boolean isInfeasibleLeaf() {
      return true;
    }

    @Override
    public String toShallowString() {
      return toString();
    }
  }