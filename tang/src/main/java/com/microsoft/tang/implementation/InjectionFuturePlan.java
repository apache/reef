package com.microsoft.tang.implementation;

import com.microsoft.tang.types.Node;

public class InjectionFuturePlan<T> extends InjectionPlan<T> {

  public InjectionFuturePlan(Node name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getNumAlternatives() {
    return 1;
  }

  @Override
  public boolean isAmbiguous() {
    return false;
  }

  @Override
  public boolean isInjectable() {
    return true;
  }

  @Override
  public boolean hasFutureDependency() {
    return true;
  }

  @Override
  protected String toAmbiguousInjectString() {
    throw new UnsupportedOperationException("InjectionFuturePlan cannot be ambiguous!");
  }

  @Override
  protected String toInfeasibleInjectString() {
    throw new UnsupportedOperationException("InjectionFuturePlan is always feasible!");
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  @Override
  public String toShallowString() {
    return "InjectionFuture<"+getNode().getFullName()+">";
  }

}
