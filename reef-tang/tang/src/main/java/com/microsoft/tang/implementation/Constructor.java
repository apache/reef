/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.implementation;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;

import java.util.*;

final public class Constructor<T> extends InjectionPlan<T> {

  final ConstructorDef<T> constructor;
  final InjectionPlan<?>[] args;
  final int numAlternatives;
  final boolean isAmbiguous;
  final boolean isInjectable;

  public InjectionPlan<?>[] getArgs() {
    return args;
  }

  /**
   * Get child elements of the injection plan tree.
   * This method is inherited from the Traversable interface.
   * TODO: use ArrayList internally (and maybe for input, too).
   *
   * @return A list of injection plans for the constructor's arguments.
   */
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return Collections.unmodifiableList(Arrays.asList(this.args));
  }

  public ConstructorDef<T> getConstructorDef() {
    return constructor;
  }

  public Constructor(final ClassNode<T> classNode,
                     final ConstructorDef<T> constructor, final InjectionPlan<?>[] args) {
    super(classNode);
    this.constructor = constructor;
    this.args = args;
    int curAlternatives = 1;
    boolean curAmbiguous = false;
    boolean curInjectable = true;
    for (final InjectionPlan<?> plan : args) {
      curAlternatives *= plan.getNumAlternatives();
      curAmbiguous |= plan.isAmbiguous();
      curInjectable &= plan.isInjectable();
    }
    this.numAlternatives = curAlternatives;
    this.isAmbiguous = curAmbiguous;
    this.isInjectable = curInjectable;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ClassNode<T> getNode() {
    return (ClassNode<T>) node;
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
  public String toString() {
    final StringBuilder sb = new StringBuilder("new " + getNode().getName() + '(');
    if (args.length > 0) {
      sb.append(args[0]);
      for (int i = 1; i < args.length; i++) {
        sb.append(", " + args[i]);
      }
    }
    sb.append(')');
    return sb.toString();
  }

  private String shallowArgString(final InjectionPlan<?> arg) {
    if (arg instanceof Constructor || arg instanceof Subplan) {
      return arg.getClass().getName() + ": " + arg.getNode().getName();
    } else {
      return arg.toShallowString();
    }
  }

  @Override
  public String toShallowString() {
    final StringBuilder sb = new StringBuilder("new " + getNode().getName() + '(');
    if (args.length > 0) {
      sb.append(shallowArgString(args[0]));
      for (int i = 1; i < args.length; i++) {
        sb.append(", " + shallowArgString(args[i]));
      }
    }
    sb.append(')');
    return sb.toString();
  }

  /**
   * @return A string describing ambiguous constructor arguments.
   * @throws IllegalArgumentException if constructor is not ambiguous.
   */
  @Override
  protected String toAmbiguousInjectString() {

    if (!isAmbiguous) {
      throw new IllegalArgumentException(getNode().getFullName() + " is NOT ambiguous.");
    }

    final StringBuilder sb = new StringBuilder(
        getNode().getFullName() + " has ambiguous arguments: [ ");

    for (final InjectionPlan<?> plan : args) {
      if (plan.isAmbiguous()) {
        sb.append(plan.toAmbiguousInjectString());
      }
    }

    sb.append(']');
    return sb.toString();
  }

  @Override
  protected String toInfeasibleInjectString() {

    final List<InjectionPlan<?>> leaves = new ArrayList<>();

    for (final InjectionPlan<?> ip : args) {
      if (!ip.isFeasible()) {
        if (ip.isInfeasibleLeaf()) {
          leaves.add(ip);
        } else {
          return ip.toInfeasibleInjectString();
        }
      }
    }

    if (leaves.isEmpty()) {
      throw new IllegalArgumentException(getNode().getFullName() + " has NO infeasible leaves.");
    }

    if (leaves.size() == 1) {
      return getNode().getFullName() + " missing argument " + leaves.get(0).getNode().getFullName();
    } else {
      final StringBuffer sb = new StringBuffer(getNode().getFullName() + " missing arguments: [\n\t");
      for (final InjectionPlan<?> leaf : leaves) {
        sb.append(leaf.getNode().getFullName() + "\n\t");
      }
      sb.append(']');
      return sb.toString();
    }
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }
}
