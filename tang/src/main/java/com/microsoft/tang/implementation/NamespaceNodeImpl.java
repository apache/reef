package com.microsoft.tang.implementation;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.NamespaceNode;
import com.microsoft.tang.Node;

public class NamespaceNodeImpl<T> extends AbstractNode implements
    NamespaceNode<T> {
  private ClassNode<T> target;

  public NamespaceNodeImpl(Node root, String name, ClassNode<T> target) {
    super(root, name);
    if (target != null && (!target.getIsPrefixTarget())) {
      throw new IllegalStateException();
    }
    this.target = target;
  }

  public NamespaceNodeImpl(Node root, String name) {
    super(root, name);
  }

  @Override
  public void setTarget(ClassNode<T> target) {
    if (this.target != null) {
      throw new IllegalStateException("Attempt to set namespace target from "
          + this.target + " to " + target);
    }
    this.target = target;
    if (!target.getIsPrefixTarget()) {
      throw new IllegalStateException();
    }
  }

  @Override
  public Node getTarget() {
    return target;
  }

  @Override
  public String toString() {
    if (target != null) {
      return super.toString() + " -> " + target.toString();
    } else {
      return super.toString();
    }
  }

}