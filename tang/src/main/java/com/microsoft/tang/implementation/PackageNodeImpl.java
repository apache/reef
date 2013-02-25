package com.microsoft.tang.implementation;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;

public class PackageNodeImpl extends AbstractNode implements PackageNode {
  public PackageNodeImpl(Node parent, String name) {
    super(parent, name);
  }
}