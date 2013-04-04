package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;

public class PackageNodeImpl extends AbstractNode implements PackageNode {
  public PackageNodeImpl(Node parent, String name, String fullName) {
    super(parent, name, fullName);
  }
  /**
   * Unlike normal nodes, the root node needs to take the package name of its
   * children into account.  Therefore, we use the full name as the key when
   * we insert nodes into the root.
   */
  @Override
  public void put(Node n) {
    super.children.put(n.getFullName(), n);
  }

}