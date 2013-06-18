package com.microsoft.tang.implementation;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;

final public class Constructor<T> extends InjectionPlan<T> {

  final ConstructorDef<T> mConstructor;
  final InjectionPlan<?>[] mArgs;
  final int mNumAlternatives;
  final boolean mIsAmbiguous;
  final boolean mIsInjectable;

  public InjectionPlan<?>[] getArgs() {
    return mArgs;
  }

  /**
   * Get child elements of the injection plan tree.
   * This method is inherited from the Traversable interface.
   * TODO: use ArrayList internally (and maybe for input, too).
   * @return A list of injection plans for the mConstructor's arguments.
   */
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return Collections.unmodifiableList(Arrays.asList(this.mArgs));
  }

  public ConstructorDef<T> getConstructorDef() {
    return mConstructor;
  }

  public Constructor(final ClassNode<T> cn,
      final ConstructorDef<T> constructor, final InjectionPlan<?>[] args) {
    super(cn);
    this.mConstructor = constructor;
    this.mArgs = args;
    int numAlternatives = 1;
    boolean isAmbiguous = false;
    boolean isInjectable = true;
    for (final InjectionPlan<?> a : args) {
      numAlternatives *= a.getNumAlternatives();
      isAmbiguous |= a.isAmbiguous();
      isInjectable &= a.isInjectable();
    }
    this.mNumAlternatives = numAlternatives;
    this.mIsAmbiguous = isAmbiguous;
    this.mIsInjectable = isInjectable;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ClassNode<T> getNode() {
    return (ClassNode<T>) node;
  }

  @Override
  public int getNumAlternatives() {
    return mNumAlternatives;
  }

  @Override
  public boolean isAmbiguous() {
    return mIsAmbiguous;
  }

  @Override
  public boolean isInjectable() {
    return mIsInjectable;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("new ").append(getNode().getName()).append('(');
    if (mArgs.length > 0) {
      sb.append(mArgs[0]);
      for (int i = 1; i < mArgs.length; i++) {
        sb.append(", ").append(mArgs[i]);
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
    if (mArgs.length > 0) {
      sb.append(shallowArgString(mArgs[0]));
      for (int i = 1; i < mArgs.length; i++) {
        sb.append(", ").append(shallowArgString(mArgs[i]));
      }
    }
    return sb.append(')').toString();
  }

  @Override
  protected String toAmbiguousInjectString() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String toInfeasibleInjectString() {

    final List<InjectionPlan<?>> leaves = new ArrayList<>();

    for (final InjectionPlan<?> ip : mArgs) {
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
