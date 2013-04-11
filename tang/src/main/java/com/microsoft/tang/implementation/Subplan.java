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
      if (selectedIndex < -1 || selectedIndex >= alternatives.length) {
        throw new ArrayIndexOutOfBoundsException();
      }
      this.selectedIndex = selectedIndex;
      if(selectedIndex != -1) {
        this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
      } else {
        int numAlternatives = 0;
        for (InjectionPlan<? extends T> a : alternatives) {
          numAlternatives += a.getNumAlternatives();
        }
        this.numAlternatives = numAlternatives;
      }
    }

    public Subplan(
        Node node,
        @SuppressWarnings("unchecked") InjectionPlan<? extends T>... alternatives) {
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

    @Override
    public String toCantInjectString(int indent) {
      if(selectedIndex != -1) {
        return alternatives[selectedIndex].toCantInjectString(indent);
      } else {
        if(alternatives.length == 0) {
          return "No known implemetnations / injectable constructors for " + this.getNode();
        } else {
          StringBuffer sb = new StringBuffer(this.getNode() + " has implementations, but none were selected");
          for(InjectionPlan<?> alt : alternatives) {
            if(alt.isInjectable()) {
              sb.append(" have plan to inject " + alt.getNode().getFullName());
            } else 
              sb.append(alt.toCantInjectString(indent+1));
          }
          return sb.toString();
        }
      }
    }
  }