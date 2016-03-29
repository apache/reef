/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.types.Node;

import java.util.*;

public final class Subplan<T> extends InjectionPlan<T> {
  final InjectionPlan<? extends T>[] alternatives;
  final int numAlternatives;
  final int selectedIndex;

  @SafeVarargs
  public Subplan(final Node node, final int selectedIndex, final InjectionPlan<T>... alternatives) {
    super(node);
    this.alternatives = alternatives;
    if (selectedIndex < -1 || selectedIndex >= alternatives.length) {
      throw new ArrayIndexOutOfBoundsException("Actual value of selectedIndex is " + selectedIndex
      + ". The length of the 'alternatives' vararg is " + alternatives.length);
    }
    this.selectedIndex = selectedIndex;
    if (selectedIndex != -1) {
      this.numAlternatives = alternatives[selectedIndex].getNumAlternatives();
    } else {
      int numAlternativesSum = 0;
      for (final InjectionPlan<? extends T> a : alternatives) {
        numAlternativesSum += a.getNumAlternatives();
      }
      this.numAlternatives = numAlternativesSum;
    }
  }

  @SafeVarargs
  public Subplan(final Node node, final InjectionPlan<T>... alternatives) {
    this(node, -1, alternatives);
  }

  /**
   * Get child elements of the injection plan tree.
   * TODO: use ArrayList internally (and maybe for input, too).
   *
   * @return A list of injection sub-plans.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return (Collection) Collections.unmodifiableCollection(Arrays.asList(this.alternatives));
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
    final StringBuilder sb = new StringBuilder("[");
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
    final StringBuilder sb = new StringBuilder("[");
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
      throw new IllegalStateException("selectedIndex equals -1");
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
      final List<InjectionPlan<?>> alts = new ArrayList<>();
      final List<InjectionPlan<?>> ambig = new ArrayList<>();
      for (final InjectionPlan<?> alt : alternatives) {
        if (alt.isFeasible()) {
          alts.add(alt);
        }
        if (alt.isAmbiguous()) {
          ambig.add(alt);
        }
      }
      final StringBuffer sb = new StringBuffer("Ambiguous subplan " + getNode().getFullName());
      for (final InjectionPlan<?> alt : alts) {
        sb.append("\n  " + alt.toShallowString() + " ");
      }
      for (final InjectionPlan<?> alt : ambig) {
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
    } else if (alternatives.length == 0) {
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

}
