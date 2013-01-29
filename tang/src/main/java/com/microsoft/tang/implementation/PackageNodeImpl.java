package com.microsoft.tang.implementation;

import com.microsoft.tang.Node;
import com.microsoft.tang.PackageNode;

public class PackageNodeImpl extends AbstractNode implements PackageNode {
  public PackageNodeImpl(Node parent, String name) {
    super(parent, name);
  }
}