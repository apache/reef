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

import java.util.ArrayList;
import java.util.List;

public class ListInjectionPlan<T> extends InjectionPlan<T> {
  private final List<InjectionPlan<T>> entries = new ArrayList<>();
  private final int numAlternatives;
  private final boolean isAmbiguous;
  private final boolean isInjectable;

  public ListInjectionPlan(final Node name, final List<InjectionPlan<T>> entries) {
    super(name);
    this.entries.addAll(entries);
    int numAlternativesAccumulator = 1;
    boolean isAmbiguousAccumulator = false;
    boolean isInjectableAccumulator = true;
    for (final InjectionPlan<T> ip : entries) {
      numAlternativesAccumulator *= ip.getNumAlternatives();
      isAmbiguousAccumulator |= ip.isAmbiguous();
      isInjectableAccumulator &= ip.isInjectable();
    }
    this.numAlternatives = numAlternativesAccumulator;
    this.isAmbiguous = isAmbiguousAccumulator;
    this.isInjectable = isInjectableAccumulator;
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

  public List<InjectionPlan<T>> getEntryPlans() {
    return new ArrayList<>(this.entries);
  }

  @Override
  protected String toAmbiguousInjectString() {
    final StringBuilder sb = new StringBuilder(getNode().getFullName() + "(list) includes ambiguous plans [");
    for (final InjectionPlan<T> ip : entries) {
      if (ip.isAmbiguous()) {
        sb.append("\n" + ip.toAmbiguousInjectString());
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  protected String toInfeasibleInjectString() {
    final StringBuilder sb = new StringBuilder(getNode().getFullName() + "(list) includes infeasible plans [");
    for (final InjectionPlan<T> ip : entries) {
      if (!ip.isFeasible()) {
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
    final StringBuilder sb = new StringBuilder("list { ");
    for (final InjectionPlan<T> ip : entries) {
      sb.append("\n").append(ip.toShallowString());
    }
    sb.append("\n } ");
    return sb.toString();
  }

}
