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
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.types.NamedParameterNode;

/**
 * Build a Graphviz representation of the configuration graph.
 */
public final class GraphVisitorGraphviz extends AbstractTypedNodeVisitor implements EdgeVisitor {

  /** Legend for the configuration graph in Graphviz format */
  private static final String LEGEND =
    "  subgraph cluster_legend {\n"
    + "    label=\"Legend\";\n"
    + "    shape=box;\n"
    + "    subgraph cluster_1 {\n"
    + "      style=invis; label=\"\";\n"
    + "      ex1l [shape=point, label=\"\"]; ex1r [shape=point, label=\"\"];\n"
    + "      ex2l [shape=point, label=\"\"]; ex2r [shape=point, label=\"\"];\n"
    + "      ex3l [shape=point, label=\"\"]; ex3r [shape=point, label=\"\"];\n"
    + "      ex1l -> ex1r [style=solid, dir=back, arrowtail=diamond, label=\"contains\"];\n"
    + "      ex2l -> ex2r [style=dashed, dir=back, arrowtail=empty, label=\"implements\"];\n"
    + "      ex3l -> ex3r [style=solid, dir=back, arrowtail=normal, label=\"binds\"];\n"
    + "    }\n"
    + "    subgraph cluster_2 {\n"
    + "      style=invis; label=\"\";\n"
    + "      PackageNode [shape=folder];\n"
    + "      ClassNode [shape=box];\n"
    + "      Singleton [shape=box, style=filled];\n"
    + "      NamedParameterNode [shape=oval];\n"
    + "    }\n"
    + "  }\n";

  /** Accumulate string representation of the graph here. */
  private final transient StringBuilder mGraphStr = new StringBuilder(
    "digraph ConfigMain {\n"
    + "  rankdir=LR;\n");

  /**
   * Entire TANG configuration object.
   */
  private final transient Configuration mConfig;

  /**
   * If true, plot IS-A edges for know implementations.
   */
  private final transient boolean mShowImpl;

  /**
   * Create a new TANG configuration visitor.
   * @param aConfig Entire TANG configuration object.
   * @param aShowImpl If true, plot IS-A edges for know implementations.
   * @param aShowLegend If true, add legend to the plot.
   */
  public GraphVisitorGraphviz(final Configuration aConfig,
          final boolean aShowImpl, final boolean aShowLegend) {
    super();
    this.mConfig = aConfig;
    this.mShowImpl = aShowImpl;
    if (aShowLegend) {
      this.mGraphStr.append(LEGEND);
    }
  }

  /**
   * @return TANG configuration represented as a Graphviz DOT string.
   */
  @Override
  public String toString() {
    return mGraphStr.toString() + "}\n";
  }

  /**
   * Process current class configuration node.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final ClassNode aNode) {

    mGraphStr.append("  ")
             .append(aNode.getName())
             .append(" [label=\"")
             .append(aNode.getName())
             .append("\", shape=box")
             .append(mConfig.isSingleton(aNode) ? ", style=filled" : "")
             .append("];\n");

    final ClassNode boundImplNode = mConfig.getBoundImplementation(aNode);
    if (boundImplNode != null) {
      mGraphStr.append("  ")
               .append(aNode.getName())
               .append(" -> ")
               .append(boundImplNode.getName())
               .append(" [style=solid, dir=back, arrowtail=normal];\n");
    }

    if (mShowImpl) {
      for (final Object implNode : aNode.getKnownImplementations()) {
        if (implNode != boundImplNode && implNode != aNode) {
          mGraphStr.append("  ")
                   .append(aNode.getName())
                   .append(" -> ")
                   .append(((ClassNode) implNode).getName())
                   .append(" [style=dashed, dir=back, arrowtail=empty];\n");
        }
      }
    }

    return true;
  }

  /**
   * Process current package configuration node.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final PackageNode aNode) {
    if (!aNode.getName().isEmpty()) {
      mGraphStr.append("  ")
               .append(aNode.getName())
               .append(" [label=\"")
               .append(aNode.getFullName())
               .append("\", shape=folder];\n");
    }
    return true;
  }

  /**
   * Process current configuration node for the named parameter.
   * @param aNode Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final NamedParameterNode aNode) {
    mGraphStr.append("  ")
             .append(aNode.getName())
             .append(" [label=\"")
             .append(aNode.getSimpleArgName())           // parameter type, e.g. "Integer"
             .append("\\n")
             .append(aNode.getName())                    // short name, e.g. "NumberOfThreads"
             .append(" = ")
             .append(mConfig.getNamedParameter(aNode))   // bound value, e.g. "16"
             .append("\\n(default = ")
             .append(aNode.getDefaultInstanceAsString()) // default value, e.g. "4"
             .append(")\", shape=oval];\n");
    return true;
  }

  /**
   * Process current edge of the configuration graph.
   * @param aNodeFrom Current configuration node.
   * @param aNodeTo Destination configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Node aNodeFrom, final Node aNodeTo) {
    if (!aNodeFrom.getName().isEmpty()) {
      mGraphStr.append("  ")
               .append(aNodeFrom.getName())
               .append(" -> ")
               .append(aNodeTo.getName())
               .append(" [style=solid, dir=back, arrowtail=diamond];\n");
    }
    return true;
  }

  /**
   * Produce a Graphviz DOT string for a given TANG configuration.
   * @param aConfig TANG configuration object.
   * @param aShowImpl If true, plot IS-A edges for know implementations.
   * @param aShowLegend If true, add legend to the plot.
   * @return configuration graph represented as a string in Graphviz DOT format.
   */
  public static String getGraphvizStr(final Configuration aConfig,
          final boolean aShowImpl, final boolean aShowLegend)
  {
    final GraphVisitorGraphviz visitor = new GraphVisitorGraphviz(aConfig, aShowImpl, aShowLegend);
    Walk.preorder(visitor, visitor, aConfig);
    return visitor.toString();
  }
}
