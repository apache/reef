package com.microsoft.tang;

public interface NamespaceNode<T> extends Node {

  public void setTarget(ClassNode<T> target);

  public Node getTarget();

}