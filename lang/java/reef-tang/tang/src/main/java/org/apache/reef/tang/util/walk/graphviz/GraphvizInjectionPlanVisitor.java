/**
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
package org.apache.reef.tang.util.walk.graphviz;

import org.apache.reef.tang.implementation.Constructor;
import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.implementation.Subplan;
import org.apache.reef.tang.implementation.java.JavaInstance;
import org.apache.reef.tang.util.walk.AbstractInjectionPlanNodeVisitor;
import org.apache.reef.tang.util.walk.EdgeVisitor;
import org.apache.reef.tang.util.walk.Walk;

/**
 * Build a Graphviz representation of the injection plan graph.
 */
public final class GraphvizInjectionPlanVisitor
    extends AbstractInjectionPlanNodeVisitor implements EdgeVisitor<InjectionPlan<?>> {
  /**
   * Legend for the configuration graph in Graphviz format
   */
  private static final String LEGEND =
      "  subgraph cluster_legend {\n"
          + "    shape=box;\n"
          + "    label=\"Legend\";\n"
          + "    Constructor [shape=box];\n"
          + "    JavaInstance [shape=box, style=bold];\n"
          + "    Subplan [shape=oval, style=dashed];\n"
          + "    RequiredSingleton [shape=box, style=filled];\n"
          + "    Subplan -> Constructor -> RequiredSingleton -> JavaInstance [style=invis];\n"
          + "  }\n";

  /**
   * Accumulate string representation of the graph here.
   */
  private final transient StringBuilder graphStr =
      new StringBuilder("digraph InjectionPlanMain {\n");

  /**
   * Create a new visitor to build a graphviz string for the injection plan.
   *
   * @param aShowLegend if true, show legend on the graph.
   */
  public GraphvizInjectionPlanVisitor(final boolean showLegend) {
    if (showLegend) {
      this.graphStr.append(LEGEND);
    }
    this.graphStr.append("subgraph cluster_main {\n  style=invis;\n");
  }

  /**
   * Produce a Graphviz DOT string for a given TANG injection plan.
   *
   * @param injectionPlan TANG injection plan.
   * @param showLegend    if true, show legend on the graph.
   * @return Injection plan represented as a string in Graphviz DOT format.
   */
  public static String getGraphvizString(
      final InjectionPlan<?> injectionPlan, final boolean showLegend) {
    final GraphvizInjectionPlanVisitor visitor = new GraphvizInjectionPlanVisitor(showLegend);
    Walk.preorder(visitor, visitor, injectionPlan);
    return visitor.toString();
  }

  /**
   * Process current injection plan node of Constructor type.
   *
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Constructor<?> node) {
    this.graphStr
        .append("  \"")
        .append(node.getClass())
        .append('_')
        .append(node.getNode().getName())
        .append("\" [label=\"")
        .append(node.getNode().getName())
        .append("\", shape=box];\n");
    return true;
  }

  /**
   * Process current injection plan node of JavaInstance type.
   *
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final JavaInstance<?> node) {
    this.graphStr
        .append("  \"")
        .append(node.getClass())
        .append('_')
        .append(node.getNode().getName())
        .append("\" [label=\"")
        .append(node.getNode().getName())
        .append(" = ")
        .append(node.getInstanceAsString())
        .append("\", shape=box, style=bold];\n");
    return true;
  }

  /**
   * Process current injection plan node of Subplan type.
   *
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Subplan<?> node) {
    this.graphStr
        .append("  \"")
        .append(node.getClass())
        .append('_')
        .append(node.getNode().getName())
        .append("\" [label=\"")
        .append(node.getNode().getName())
        .append("\", shape=oval, style=dashed];\n");
    return true;
  }

  /**
   * Process current edge of the injection plan.
   *
   * @param nodeFrom Current injection plan node.
   * @param nodeTo   Destination injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final InjectionPlan<?> nodeFrom, final InjectionPlan<?> nodeTo) {
    this.graphStr
        .append("  \"")
        .append(nodeFrom.getClass())
        .append('_')
        .append(nodeFrom.getNode().getName())
        .append("\" -> \"")
        .append(nodeTo.getClass())
        .append('_')
        .append(nodeTo.getNode().getName())
        .append("\" [style=solid];\n");
    return true;
  }

  /**
   * @return TANG injection plan represented as a Graphviz DOT string.
   */
  @Override
  public String toString() {
    return this.graphStr.toString() + "}}\n";
  }
}
