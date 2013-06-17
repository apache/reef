/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.util.walk;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.types.NamedParameterNode;

/**
 * Generic interface to traverse nodes of the configuration graph.
 * Dispatches between ClassNode, PackageNode, and NamedParameterNode types.
 * It is used e.g. in Walk.preorder()
 */
public abstract class AbstractTypedNodeVisitor implements NodeVisitor<Node> {

  /**
   * Manually dispatch between different types of Nodes and call a proper visit() method.
   * Currently dispatches between ClassNode, PackageNode, and NamedParameterNode types.
   * @param aNode TANG configuration node.
   * @return true to proceed with the next node, false to cancel.
   * @throws ClassCastException if Node is not one of ClassNode, PackageNode,
   * or NamedParameterNode.
   */
  @Override
  public boolean visit(final Node aNode) {
    if (aNode instanceof ClassNode) {
      return visit((ClassNode) aNode);
    } else if (aNode instanceof PackageNode) {
      return visit((PackageNode) aNode);
    } else if (aNode instanceof NamedParameterNode) {
      return visit((NamedParameterNode) aNode);
    }
    throw new ClassCastException(
        "Node " + aNode.getClass() + " cannot be casted to one of the known subclasses."
        + " Override this method to handle the case.");
  }

  /**
   * Process current configuration node of ClassNode type.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(ClassNode aNode);

  /**
   * Process current configuration node of PackageNode type.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(PackageNode aNode);

  /**
   * Process current configuration node of NamedParameterNode type.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(NamedParameterNode aNode);
}
