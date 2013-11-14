package com.microsoft.tang.implementation;

import java.util.Set;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicHashSet;

public class SetInjectionPlan<T> extends InjectionPlan<T> {
  private final Set<InjectionPlan<T>> entries = new MonotonicHashSet<>();
  private final int numAlternatives;
  private final boolean isAmbiguous;
  private final boolean isInjectable;
  public SetInjectionPlan(Node name, Set<InjectionPlan<T>> entries) {
    super(name);
    this.entries.addAll(entries);
    int numAlternatives = 1;
    boolean isAmbiguous = false;
    boolean isInjectable = true;
    for(InjectionPlan<T> ip : entries) {
      numAlternatives *= ip.getNumAlternatives();
      isAmbiguous |= ip.isAmbiguous();
      isInjectable &= ip.isInjectable();
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
  @Deprecated
  public boolean hasFutureDependency() {
    return false;
  }
  
  public Set<InjectionPlan<T>> getEntryPlans() {
    return new MonotonicHashSet<>(this.entries);
  }
  

  @Override
  protected String toAmbiguousInjectString() {
    StringBuilder sb = new StringBuilder(getNode().getFullName() + "(set) includes ambiguous plans [");
    for(InjectionPlan<T> ip : entries) {
      if(ip.isAmbiguous()) {
        sb.append("\n" + ip.toAmbiguousInjectString());
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  protected String toInfeasibleInjectString() {
    StringBuilder sb = new StringBuilder(getNode().getFullName() + "(set) includes infeasible plans [");
    for(InjectionPlan<T> ip : entries) {
      if(!ip.isFeasible()) {
        sb.append("\n" + ip.toInfeasibleInjectString());
      }
    }
    sb.append("\n]");
    return sb.toString();
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  @Override
  public String toShallowString() {
        StringBuilder sb = new StringBuilder("set { ");
        for(InjectionPlan<T> ip : entries) {
            sb.append("\n" + ip.toShallowString());
        }
        sb.append("\n } ");
        return null;
    }

}
