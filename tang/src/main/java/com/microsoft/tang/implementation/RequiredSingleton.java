package com.microsoft.tang.implementation;

import java.util.Collection;
import java.util.Collections;

import com.microsoft.tang.types.Node;

public class RequiredSingleton<T,U> extends InjectionPlan<T> {
  public RequiredSingleton(Node node, InjectionPlan<U> preReq) {
    super(node);
    this.preReq = preReq;
  }

  final InjectionPlan<U> preReq;

  /**
   * Get child elements of the injection plan tree.
   * @return A list with single prerequisite injection plan.
   */
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return (Collection) Collections.singletonList(this.preReq);
  }

  @Override
  public int getNumAlternatives() {
    return preReq.getNumAlternatives();
  }

  @Override
  public boolean isAmbiguous() {
    return preReq.isAmbiguous();
  }

  @Override
  public boolean isInjectable() {
    return preReq.isInjectable();
  }

  @Override
  protected String toAmbiguousInjectString() {
    return preReq.toAmbiguousInjectString();
  }

  @Override
  protected String toInfeasibleInjectString() {
    return preReq.toInfeasibleInjectString();
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return preReq.isInfeasibleLeaf();
  }

  @Override
  public String toShallowString() {
    return toString();
  }
  
}
