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
package com.microsoft.tang.util.walk;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.types.NamedParameterNode;

/**
 * Generic interface to traverse nodes of the class hierarchy.
 * Dispatches between ClassNode, PackageNode, and NamedParameterNode types.
 * It is used e.g. in Walk.preorder()
 */
public abstract class AbstractClassHierarchyNodeVisitor implements NodeVisitor<Node> {

  /**
   * Manually dispatch between different types of Nodes and call a proper visit() method.
   * Currently dispatches between ClassNode, PackageNode, and NamedParameterNode types.
   * @param node TANG configuration node.
   * @return true to proceed with the next node, false to cancel.
   * @throws ClassCastException if Node is not one of ClassNode, PackageNode,
   * or NamedParameterNode.
   */
  @Override
  public boolean visit(final Node node) {
    if (node instanceof ClassNode) {
      return visit((ClassNode<?>) node);
    } else if (node instanceof PackageNode) {
      return visit((PackageNode) node);
    } else if (node instanceof NamedParameterNode) {
      return visit((NamedParameterNode<?>) node);
    }
    throw new ClassCastException(
        "Node " + node.getClass() + " cannot be casted to one of the known subclasses."
        + " Override this method to handle the case.");
  }

  /**
   * Process current configuration node of ClassNode type.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(ClassNode<?> node);

  /**
   * Process current configuration node of PackageNode type.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(PackageNode node);

  /**
   * Process current configuration node of NamedParameterNode type.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(NamedParameterNode<?> node);
}
