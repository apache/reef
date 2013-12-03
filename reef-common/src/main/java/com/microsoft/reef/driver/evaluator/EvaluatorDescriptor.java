package com.microsoft.reef.driver.evaluator;

import com.microsoft.reef.driver.catalog.NodeDescriptor;

/**
 * Metadata about an Evaluator.
 */
public interface EvaluatorDescriptor {

  /**
   * @return the NodeDescriptor of the node where this Evaluator is running.
   */
  public NodeDescriptor getNodeDescriptor();

  /**
   * @return the type of Evaluator.
   */
  public EvaluatorType getType();
}
