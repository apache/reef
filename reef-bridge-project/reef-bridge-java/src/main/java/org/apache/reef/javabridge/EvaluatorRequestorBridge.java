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

package org.apache.reef.javabridge;

import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class EvaluatorRequestorBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(EvaluatorRequestorBridge.class.getName());
  private final boolean isBlocked;
  private final EvaluatorRequestor jevaluatorRequestor;
  // accumulate how many evaluators have been submitted through this instance
  // of EvaluatorRequestorBridge
  private int clrEvaluatorsNumber;

  public EvaluatorRequestorBridge(final EvaluatorRequestor evaluatorRequestor, final boolean isBlocked) {
    this.jevaluatorRequestor = evaluatorRequestor;
    this.clrEvaluatorsNumber = 0;
    this.isBlocked = isBlocked;
  }

  public void submit(final int evaluatorsNumber, final int memory, final int virtualCore, final String rack) {
    if (this.isBlocked) {
      throw new RuntimeException("Cannot request additional Evaluator, this is probably because the Driver has crashed and restarted, and cannot ask for new container due to YARN-2433.");
    }

    if (rack != null && !rack.isEmpty()) {
      LOG.log(Level.WARNING, "Ignoring rack preference.");
    }

    clrEvaluatorsNumber += evaluatorsNumber;

    final EvaluatorRequest request = EvaluatorRequest.newBuilder()
        .setNumber(evaluatorsNumber)
        .setMemory(memory)
        .setNumberOfCores(virtualCore)
        .build();

    LOG.log(Level.FINE, "submitting evaluator request {0}", request);
    jevaluatorRequestor.submit(request);
  }

  public int getEvaluatorNumber() {
    return clrEvaluatorsNumber;
  }

  @Override
  public void close() {
  }
}