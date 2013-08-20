package com.microsoft.tang.implementation;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.microsoft.tang.types.Node;

final public class Subplan<T> extends InjectionPlan<T> {
  final InjectionPlan<? extends T>[] alternatives;
  final int numAlternatives;
  final int selectedIndex;

  @SafeVarargs
  public Subplan(Node node, int selectedIndex, InjectionPlan<T>... alternatives) {
    super(node);
    this.alternatives = alternatives;
    if (selectedIndex < -1 || selectedIndex >= alternatives.length) {
      throw new ArrayIndexOutOfBoundsException();
    }
    this.selectedIndex = selectedIndex;
    if (selectedIndex != -1) {
      this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
    } else {
      int numAlternatives = 0;
      for (InjectionPlan<? extends T> a : alternatives) {
        numAlternatives += a.getNumAlternatives();
      }
      this.numAlternatives = numAlternatives;
    }
  }

  /**
   * Get child elements of the injection plan tree.
   * TODO: use ArrayList internally (and maybe for input, too).
   * @return A list of injection sub-plans.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return (Collection)Collections.unmodifiableCollection(Arrays.asList(this.alternatives));
  }

  @SafeVarargs
  public Subplan(Node node, InjectionPlan<T>... alternatives) {
    this(node, -1, alternatives);
  }

  @Override
  public int getNumAlternatives() {
    return this.numAlternatives;
  }

  /**
   * Even if there is only one sub-plan, it was registered as a default plan,
   * and is therefore ambiguous.
   */
  @Override
  public boolean isAmbiguous() {
    if (selectedIndex == -1) {
      return true;
    }
    return alternatives[selectedIndex].isAmbiguous();
  }

  @Override
  public boolean isInjectable() {
    if (selectedIndex == -1) {
      return false;
    } else {
      return alternatives[selectedIndex].isInjectable();
    }
  }

  @Override
  public String toString() {
    if (alternatives.length == 1) {
      return getNode().getName() + " = " + alternatives[0];
    } else if (alternatives.length == 0) {
      return getNode().getName() + ": no injectable constructors";
    }
    StringBuilder sb = new StringBuilder("[");
    sb.append(getNode().getName() + " = " + alternatives[0]);
    for (int i = 1; i < alternatives.length; i++) {
      sb.append(" | " + alternatives[i]);
    }
    sb.append("]");
    return sb.toString();
  }
  @Override
  public String toShallowString() {
    if (alternatives.length == 1) {
      return getNode().getName() + " = " + alternatives[0].toShallowString();
    } else if (alternatives.length == 0) {
      return getNode().getName() + ": no injectable constructors";
    }
    StringBuilder sb = new StringBuilder("[");
    sb.append(getNode().getName() + " = " + alternatives[0].toShallowString());
    for (int i = 1; i < alternatives.length; i++) {
      sb.append(" | " + alternatives[i].toShallowString());
    }
    sb.append("]");
    return sb.toString();
  }
  public int getSelectedIndex() {
    return selectedIndex;
  }
  public InjectionPlan<? extends T> getDelegatedPlan() {
    if (selectedIndex == -1) {
      throw new IllegalStateException();
    } else {
      return alternatives[selectedIndex];
    }
  }

  @Override
  protected String toAmbiguousInjectString() {
    if (alternatives.length == 1) {
      return alternatives[0].toAmbiguousInjectString();
    } else if (selectedIndex != -1) {
      return alternatives[selectedIndex].toAmbiguousInjectString();
    } else {
      List<InjectionPlan<?>> alts = new ArrayList<>();
      List<InjectionPlan<?>> ambig = new ArrayList<>();
      for (InjectionPlan<?> alt : alternatives) {
        if (alt.isFeasible()) {
          alts.add(alt);
        }
        if (alt.isAmbiguous()) {
          ambig.add(alt);
        }
      }
      StringBuffer sb = new StringBuffer("Ambigous subplan " + getNode().getFullName());
      for(InjectionPlan<?> alt: alts) {
        sb.append("\n  " + alt.toShallowString() + " ");
      }
      for(InjectionPlan<?> alt: ambig) {
        sb.append("\n  " + alt.toShallowString() + " ");
      }
      sb.append("\n]");
      return sb.toString();
    }
  }

  @Override
  protected String toInfeasibleInjectString() {
    if (alternatives.length == 1) {
      return alternatives[0].toInfeasibleInjectString();
    } else if(alternatives.length == 0) {
      return "No known implementations / injectable constructors for "
          + this.getNode().getFullName();
    } else if (selectedIndex != -1) {
      return alternatives[selectedIndex].toInfeasibleInjectString();
    } else {
      return "Multiple infeasible plans: " + toPrettyString();
    }
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  public InjectionPlan<?>[] getPlans() {
    return Arrays.copyOf(alternatives, alternatives.length);
  }

  @Override
  @Deprecated
  public boolean hasFutureDependency() {
    if(selectedIndex == -1) {
      throw new IllegalStateException("hasFutureDependency() called on ambiguous subplan!");
    }
    return alternatives[selectedIndex].hasFutureDependency();
  }

}