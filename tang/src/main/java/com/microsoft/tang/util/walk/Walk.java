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

import com.microsoft.tang.Configuration;
import com.microsoft.tang.types.Node;

/**
 *
 * @author sergiym
 */
public final class Walk {

  /**
   * This is a utility class that has only static methods - do not instantiate.
   * @throws IllegalAccessException always.
   */
  private Walk() throws IllegalAccessException {
    throw new IllegalAccessException("Do not instantiate this class.");
  }

  /**
   * Traverse the entire configuration tree in preorder.
   * @param aVisitor node and edge visitor.
   * @param aConfig configuration to process.
   * @return true if all nodes has been walked, false if visitor stopped early.
   */
  public static boolean preorder(final GraphVisitor aVisitor, final Configuration aConfig) {
    final Node root = aConfig.getClassHierarchy().getNamespace();
    return preorder(aVisitor, root);
  }

  /**
   * Traverse the configuration (sub)tree in preorder, starting from the given node.
   * @param aVisitor node and edge visitor.
   * @param aNode current node of the configuration tree.
   * @return true if all nodes has been walked, false if visitor stopped early.
   */
  public static boolean preorder(final GraphVisitor aVisitor, final Node aNode) {
    if (aVisitor.processNode(aNode)) {
      for (final Node child : aNode.getChildren()) {
        if (!(aVisitor.processEdge(aNode, child) && preorder(aVisitor, child))) {
          return false;
        }
      }
    }
    return true;
  }
}
