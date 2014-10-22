/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.resourcemanager;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManagerFactory;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
 * about the current state of a given resource. Ideally, we should think the same thing.
 */
@Private
public final class ResourceStatusHandler implements EventHandler<DriverRuntimeProtocol.ResourceStatusProto> {

  private final Evaluators evaluators;
  private final EvaluatorManagerFactory evaluatorManagerFactory;

  @Inject
  ResourceStatusHandler(final Evaluators evaluators, final EvaluatorManagerFactory evaluatorManagerFactory) {
    this.evaluators = evaluators;
    this.evaluatorManagerFactory = evaluatorManagerFactory;
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
      if(resourceStatusProto.getIsFromPreviousDriver()){
        EvaluatorManager previousEvaluatorManager = this.evaluatorManagerFactory.createForEvaluatorFailedDuringDriverRestart(resourceStatusProto);
        previousEvaluatorManager.onResourceStatusMessage(resourceStatusProto);
      } else {
        throw new RuntimeException(
            "Unknown resource status from evaluator " + resourceStatusProto.getIdentifier() +
                " with state " + resourceStatusProto.getState()
        );
      }
    }
  }
}
