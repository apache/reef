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
package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;

public class PackageNodeImpl extends AbstractNode implements PackageNode {
  public PackageNodeImpl(Node parent, String name, String fullName) {
    super(parent, name, fullName);
  }
  public PackageNodeImpl() {
    this(null, "", "[root node]");
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