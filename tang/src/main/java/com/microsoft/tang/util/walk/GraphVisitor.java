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

/**
 * Generic interface for configuration graph walk.
 * It is used e.g. in Walk.preorder()
 * TODO: currently it assumes that the configuration graph is a tree!
 * @author sergiym
 */
public interface GraphVisitor {

  /**
   * Process current configuration node.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  boolean processNode(Node aNode);

  /**
   * Process current edge of the configuration graph.
   * @param aNodeFrom Current configuration node.
   * @param aNodeTo Destination configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  boolean processEdge(Node aNodeFrom, Node aNodeTo);
}
