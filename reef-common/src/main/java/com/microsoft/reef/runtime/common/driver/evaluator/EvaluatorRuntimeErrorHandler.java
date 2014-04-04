/**
 * Copyright (C) 201 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The error handler receives all runtime errors from all evaluators in the system.
 * Its primary function is to dispatch these to the appropriate EvaluatorManager.
 */
@Private
public final class EvaluatorRuntimeErrorHandler implements EventHandler<RemoteMessage<ReefServiceProtos.RuntimeErrorProto>> {
  private static final Logger LOG = Logger.getLogger(EvaluatorRuntimeErrorHandler.class.toString());
  private final Evaluators evaluators;


  @Inject
  EvaluatorRuntimeErrorHandler(final Evaluators evaluators) {
    this.evaluators = evaluators;
  }

  @Override
  public void onNext(final RemoteMessage<ReefServiceProtos.RuntimeErrorProto> runtimeErrorProtoRemoteMessage) {
    final ReefServiceProtos.RuntimeErrorProto runtimeErrorProto = runtimeErrorProtoRemoteMessage.getMessage();
    final FailedRuntime error = new FailedRuntime(runtimeErrorProto);
    LOG.log(Level.WARNING, "Runtime error: " + error, error.getCause());

    final EvaluatorException evaluatorException = error.getCause() != null ?
        new EvaluatorException(error.getId(), error.getCause()) :
        new EvaluatorException(error.getId(), "Runtime error");

    final String evaluatorId = error.getId();
    final Optional<EvaluatorManager> evaluatorManager = this.evaluators.get(evaluatorId);
    if (evaluatorManager.isPresent()) {
      evaluatorManager.get().onEvaluatorException(evaluatorException);
    } else {
      LOG.log(Level.WARNING, "Unknown evaluator runtime error: " + error, error.getCause());
    }
  }
}
