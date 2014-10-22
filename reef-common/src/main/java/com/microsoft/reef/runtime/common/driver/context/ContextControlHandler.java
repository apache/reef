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
package com.microsoft.reef.runtime.common.driver.context;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorControlHandler;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles context control messages.
 */
@DriverSide
@Private
public class ContextControlHandler {
  private static final Logger LOG = Logger.getLogger(ContextControlHandler.class.getName());
  private final EvaluatorControlHandler evaluatorControlHandler;
  private final String evaluatorId;

  /**
   * @param evaluatorControlHandler used to send the actual evaluator control messages.
   * @param evaluatorId             the ID of the evaluator this ContextControlHandler communicates with.
   */
  @Inject
  ContextControlHandler(final EvaluatorControlHandler evaluatorControlHandler,
                        final @Parameter(EvaluatorManager.EvaluatorIdentifier.class) String evaluatorId) {
    this.evaluatorControlHandler = evaluatorControlHandler;
    this.evaluatorId = evaluatorId;
    LOG.log(Level.FINE, "Instantiated 'ContextControlHandler'");
  }

  public synchronized void send(final EvaluatorRuntimeProtocol.ContextControlProto contextControlProto) {
    final EvaluatorRuntimeProtocol.EvaluatorControlProto evaluatorControlProto =
        EvaluatorRuntimeProtocol.EvaluatorControlProto.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setIdentifier(evaluatorId)
            .setContextControl(contextControlProto).build();
    this.evaluatorControlHandler.send(evaluatorControlProto);
  }
}
