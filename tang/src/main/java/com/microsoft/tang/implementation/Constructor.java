package com.microsoft.tang.implementation;

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
   * TODO: use ArrayList internally (and maybe for input, too).
   * @return A list of injection plans for the constructor's arguments.
   */
  @Override
  public List<InjectionPlan<?>> getChildren() {
    return Collections.unmodifiableList(Arrays.asList(this.args));
  }

  public ConstructorDef<T> getConstructorDef() {
    return constructor;
  }

  public Constructor(final ClassNode<T> cn,
      final ConstructorDef<T> constructor, final InjectionPlan<?>[] args) {
    super(cn);
    this.constructor = constructor;
    this.args = args;
    int numAlternatives = 1;
    boolean isAmbiguous = false;
    boolean isInjectable = true;
    for (InjectionPlan<?> a : args) {
      numAlternatives *= a.getNumAlternatives();
      if (a.isAmbiguous())
        isAmbiguous = true;
      if (!a.isInjectable())
        isInjectable = false;
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
    StringBuilder sb = new StringBuilder("new " + getNode().getName() + "(");
    if (args.length == 0) {
    } else if (args.length == 1) {
      sb.append(args[0]);
    } else {
      sb.append(args[0]);
      for (int i = 1; i < args.length; i++) {
        sb.append(", " + args[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  private String shallowArgString(InjectionPlan<?> arg) {
    if((arg instanceof Constructor) || (arg instanceof Subplan)) {
      return arg.getClass().getName() + ": " + arg.getNode().getName();
    } else {
      return arg.toShallowString();
    }
  }
  @Override
  public String toShallowString() {
    StringBuilder sb = new StringBuilder("new " + getNode().getName() + "(");
    if (args.length == 0) {
    } else if (args.length == 1) {
      sb.append(shallowArgString(args[0]));
    } else {
      sb.append(shallowArgString(args[0]));
      for (int i = 1; i < args.length; i++) {
        sb.append(", " + shallowArgString(args[i]));
      }
    }
    sb.append(")");
    return sb.toString();
  }

  
  @Override
  protected String toAmbiguousInjectString() {
    throw new UnsupportedOperationException();
  }
  @Override
  protected String toInfeasibleInjectString() {
    List<InjectionPlan<?>> leaves = new ArrayList<>();
    for(InjectionPlan<?> ip : args) {
      if(!ip.isFeasible()) {
        if(ip.isInfeasibleLeaf()) {
          leaves.add(ip);
        } else {
          return ip.toInfeasibleInjectString();
        }
      }
    }
    if(leaves.size() == 1) {
     return getNode().getFullName() + " missing argument " + leaves.get(0).getNode().getFullName(); 
    } else {
      StringBuffer sb = new StringBuffer(getNode().getFullName() + " missing arguments: [ ");
      for(InjectionPlan<?> leaf : leaves) {
        sb.append(leaf.getNode().getFullName() + " ");
      }
      sb.append("]");
      return sb.toString();
    }
  }
  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

}