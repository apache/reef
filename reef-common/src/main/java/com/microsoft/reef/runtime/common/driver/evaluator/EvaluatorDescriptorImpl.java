package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;

@Private
@DriverSide
public final class EvaluatorDescriptorImpl implements EvaluatorDescriptor {

  private final NodeDescriptor nodeDescriptor;
  private final EvaluatorType type;

  public EvaluatorDescriptorImpl(final NodeDescriptor nodeDescriptor, final EvaluatorType type) {
    this.nodeDescriptor = nodeDescriptor;
    this.type = type;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return this.nodeDescriptor;
  }

  @Override
  public EvaluatorType getType() {
    return this.type;
  }
}
