package com.microsoft.tang.implementation;

import com.microsoft.tang.types.Node;

final public class Subplan<T> extends InjectionPlan<T> {
    final InjectionPlan<? extends T>[] alternatives;
    final int numAlternatives;
    final int selectedIndex;

    public Subplan(
        Node node,
        int selectedIndex,
        @SuppressWarnings("unchecked") InjectionPlan<? extends T>... alternatives) {
      super(node);
      this.alternatives = alternatives;
      if (selectedIndex < 0 || selectedIndex >= alternatives.length) {
        throw new ArrayIndexOutOfBoundsException();
      }
      this.selectedIndex = selectedIndex;
      this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
    }

    public Subplan(
        Node node,
        @SuppressWarnings("unchecked") InjectionPlan<? extends T>... alternatives) {
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
      if(alternatives.length == 1) {
        return getNode().getName() + " = " + alternatives[0];
      } else if(alternatives.length == 0) {
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

    public InjectionPlan<? extends T> getDelegatedPlan() {
      if (selectedIndex == -1) {
        throw new IllegalStateException();
      } else {
        return alternatives[selectedIndex];
      }
    }
  }