/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.util.walk;

import org.apache.reef.tang.types.Traversable;

/**
 * Generic graph traversal.
 */
public final class Walk {

  /**
   * This is a utility class that has only static methods - do not instantiate.
   *
   * @throws IllegalAccessException always.
   */
  private Walk() throws IllegalAccessException {
    throw new IllegalAccessException("Do not instantiate this class.");
  }

  /**
   * Traverse the configuration (sub)tree in preorder, starting from the given node.
   * FIXME: handle loopy graphs correctly!
   *
   * @param nodeVisitor node visitor. Can be null.
   * @param edgeVisitor edge visitor. Can be null.
   * @param node        current node of the configuration tree.
   * @return true if all nodes has been walked, false if visitor stopped early.
   */
  public static <T extends Traversable<T>> boolean preorder(
      final NodeVisitor<T> nodeVisitor, final EdgeVisitor<T> edgeVisitor, final T node) {
    if (nodeVisitor != null && nodeVisitor.visit(node)) {
      if (edgeVisitor != null) {
        for (final T child : node.getChildren()) {
          if (!(edgeVisitor.visit(node, child)
              && preorder(nodeVisitor, edgeVisitor, child))) {
            return false;
          }
        }
      } else {
        for (final T child : node.getChildren()) {
          if (!preorder(nodeVisitor, null, child)) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
