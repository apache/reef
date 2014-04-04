package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.EventHandler;

/**
 * A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
 * about the current state of a given resource. Ideally, we should think the same thing.
 */
@Private
final class ResourceStatusHandler implements EventHandler<DriverRuntimeProtocol.ResourceStatusProto> {

  private final Evaluators evaluators;

  ResourceStatusHandler(final Evaluators evaluators) {
    this.evaluators = evaluators;
  }

  /**
   * This resource status message comes from the ResourceManager layer; telling me what it thinks
   * about the state of the resource executing an Evaluator; This method simply passes the message
   * off to the referenced EvaluatorManager
   *
   * @param resourceStatusProto resource status message from the ResourceManager
   */
  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    final Optional<EvaluatorManager> evaluatorManager = this.evaluators.get(resourceStatusProto.getIdentifier());
    if (evaluatorManager.isPresent()) {
      evaluatorManager.get().onResourceStatusMessage(resourceStatusProto);
    } else {
      throw new RuntimeException(
          "Unknown resource status from evaluator " + resourceStatusProto.getIdentifier() +
              " with state " + resourceStatusProto.getState()
      );
    }
  }
}
