package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Handles new resource allocations by adding a new EvaluatorManager.
 */
@Private
public final class ResourceAllocationHandler implements EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> {
  private final Evaluators evaluators;

  @Inject
  ResourceAllocationHandler(final Evaluators evaluators) {
    this.evaluators = evaluators;
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceAllocationProto value) {
    evaluators.addEvaluatorManagerFromAllocation(value);
  }
}