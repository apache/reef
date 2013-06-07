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
 * Build a Graphviz representation of the configuration graph.
 * @author sergiym
 */
public final class GraphVisitorGraphviz implements GraphVisitor {

  /** Entire TANG configuration graph object. */
  private final transient Configuration mConfig;

  /** Accumulate string representation of the graph here. */
  private final transient StringBuilder mGraphStr = new StringBuilder("digraph G {\n");

  /**
   * Initialize the graph visitor.
   * @param aConfig Entire TANG configuration graph object.
   */
  public GraphVisitorGraphviz(final Configuration aConfig) {
    this.mConfig = aConfig;
  }

  /**
   * @return Config represented as a Graphviz DOT string.
   */
  @Override
  public String toString() {
    return this.mGraphStr.toString() + "}\n";
  }

  /**
   * Process current configuration node.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean processNode(final Node aNode) {
    this.mGraphStr.append("  \"").append(aNode.getName()).append("\";\n");
    return true;
  }

  /**
   * Process current edge of the configuration graph.
   * @param aNodeFrom Current configuration node.
   * @param aNodeTo Destination configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean processEdge(final Node aNodeFrom, final Node aNodeTo) {
    this.mGraphStr.append("  \"").append(aNodeFrom.getName())
                  .append("\" --> \"").append(aNodeTo.getName()).append("\";\n");
    return true;
  }
}
