package com.microsoft.tang.implementation;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;

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
   * @return A list of injection plans for the Constructor's arguments.
   */
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return Collections.unmodifiableList(Arrays.asList(this.args));
  }

  public ConstructorDef<T> getConstructorDef() {
    return constructor;
  }

  public Constructor(final ClassNode<T> aNode,
      final ConstructorDef<T> aConstructor, final InjectionPlan<?>[] aArgs) {
    super(aNode);
    this.constructor = aConstructor;
    this.args = aArgs;
    int numAlternatives = 1;
    boolean isAmbiguous = false;
    boolean isInjectable = true;
    for (final InjectionPlan<?> plan : aArgs) {
      numAlternatives *= plan.getNumAlternatives();
      isAmbiguous |= plan.isAmbiguous();
      isInjectable &= plan.isInjectable();
    }
    this.numAlternatives = numAlternatives;
    this.isAmbiguous = isAmbiguous;
    this.isInjectable = isInjectable;
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
    final StringBuilder sb = new StringBuilder("new ").append(getNode().getName()).append('(');
    if (args.length > 0) {
      sb.append(args[0]);
      for (int i = 1; i < args.length; i++) {
        sb.append(", ").append(args[i]);
      }
    }
    return sb.append(')').toString();
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
    final StringBuilder sb = new StringBuilder("new ").append(getNode().getName()).append('(');
    if (args.length > 0) {
      sb.append(shallowArgString(args[0]));
      for (int i = 1; i < args.length; i++) {
        sb.append(", ").append(shallowArgString(args[i]));
      }
    }
    return sb.append(')').toString();
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

    final StringBuilder sb =
        new StringBuilder(getNode().getFullName()).append(" has ambiguous arguments: [ ");

    for (final InjectionPlan<?> plan : args) {
      if (plan.isAmbiguous()) {
        sb.append(plan.getNode().getFullName()).append(' ');
      }
    }

    return sb.append(']').toString();
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

    if (leaves.size() == 1) {
     return getNode().getFullName() + " missing argument " + leaves.get(0).getNode().getFullName(); 
    }

    final StringBuffer sb =
        new StringBuffer(getNode().getFullName()).append(" missing arguments: [ ");

    for (final InjectionPlan<?> leaf : leaves) {
      sb.append(leaf.getNode().getFullName()).append(' ');
    }

    return sb.append(']').toString();
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }
}
