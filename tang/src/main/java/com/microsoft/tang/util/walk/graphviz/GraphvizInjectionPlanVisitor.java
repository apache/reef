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
package com.microsoft.tang.util.walk.graphviz;

import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.Constructor;
import com.microsoft.tang.implementation.RequiredSingleton;
import com.microsoft.tang.implementation.Subplan;
import com.microsoft.tang.implementation.java.JavaInstance;

import com.microsoft.tang.util.walk.Walk;
import com.microsoft.tang.util.walk.EdgeVisitor;
import com.microsoft.tang.util.walk.AbstractInjectionPlanNodeVisitor;

/**
 * Build a Graphviz representation of the injection plan graph.
 */
public final class GraphvizInjectionPlanVisitor
    extends AbstractInjectionPlanNodeVisitor implements EdgeVisitor<InjectionPlan<?>>
{
  /** Accumulate string representation of the graph here. */
  private final transient StringBuilder mGraphStr =
          new StringBuilder("digraph InjectionPlanMain {\n");

  /**
   * Process current injection plan node of Constructor type.
   * @param aNode Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Constructor<?> aNode) {
    mGraphStr.append("  ")
             .append(aNode.getNode().getName())
             .append(" [label=\"")
             .append(aNode.getNode().getName())
             .append("\", shape=box];\n");
    return true;
  }

  /**
   * Process current injection plan node of JavaInstance type.
   * @param aNode Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final JavaInstance<?> aNode) {
    mGraphStr.append("  ")
             .append(aNode.getNode().getName())
             .append(" [label=\"")
             .append(aNode.getNode().getName())
             .append(" = ")
             .append(aNode.getInstanceAsString())
             .append("\", shape=box, style=bold];\n");
    return true;
  }

  /**
   * Process current injection plan node of RequiredSingleton type.
   * @param aNode Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final RequiredSingleton<?,?> aNode) {
    mGraphStr.append("  ")
             .append(aNode.getNode().getName())
             .append(" [label=\"")
             .append(aNode.getNode().getName())
             .append("\", shape=box, style=filled];\n");
    return true;
  }

  /**
   * Process current injection plan node of Subplan type.
   * @param aNode Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Subplan<?> aNode) {
    mGraphStr.append("  ")
             .append(aNode.getNode().getName())
             .append(" [label=\"")
             .append(aNode.getNode().getName())
             .append("\", shape=oval, style=dashed];\n");
    return true;
  }

  /**
   * Process current edge of the injection plan.
   * @param aNodeFrom Current injection plan node.
   * @param aNodeTo Destination injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final InjectionPlan<?> aNodeFrom, final InjectionPlan<?> aNodeTo) {
    mGraphStr.append("  ")
             .append(aNodeFrom.getNode().getName())
             .append(" -> ")
             .append(aNodeTo.getNode().getName())
             .append(" [style=solid];\n");
    return true;
  }

  /**
   * @return TANG injection plan represented as a Graphviz DOT string.
   */
  @Override
  public String toString() {
    return mGraphStr.toString() + "}\n";
  }

  /**
   * Produce a Graphviz DOT string for a given TANG injection plan.
   * @param aInjectionPlan TANG injection plan.
   * @return Injection plan represented as a string in Graphviz DOT format.
   */
  public static String getGraphvizStr(final InjectionPlan<?> aInjectionPlan)
  {
    final GraphvizInjectionPlanVisitor visitor = new GraphvizInjectionPlanVisitor();
    Walk.preorder(visitor, visitor, aInjectionPlan);
    return visitor.toString();
  }
}
