/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.javabridge;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.evaluator.FailedEvaluator}.
 */
@Private
@Interop(CppFiles = { "FailedEvaluatorClr2Java.cpp" }, CsFiles = { "IFailedEvaluatorClr2Java", "FailedEvaluator" })
public final class FailedEvaluatorBridge extends NativeBridge implements Identifiable {
  private static final Logger LOG = Logger.getLogger(FailedEvaluatorBridge.class.getName());
  private final FailedEvaluator jfailedEvaluator;
  private final EvaluatorRequestorBridge evaluatorRequestorBridge;
  private final String evaluatorId;
  private final ActiveContextBridgeFactory activeContextBridgeFactory;

  public FailedEvaluatorBridge(final FailedEvaluator failedEvaluator,
                               final EvaluatorRequestor evaluatorRequestor,
                               final boolean blockedForAdditionalEvaluator,
                               final LoggingScopeFactory loggingScopeFactory,
                               final ActiveContextBridgeFactory activeContextBridgeFactory) {
    this.jfailedEvaluator = failedEvaluator;
    this.evaluatorId = failedEvaluator.getId();
    this.evaluatorRequestorBridge =
        new EvaluatorRequestorBridge(evaluatorRequestor, blockedForAdditionalEvaluator, loggingScopeFactory);
    this.activeContextBridgeFactory = activeContextBridgeFactory;
  }

  public int getNewlyRequestedEvaluatorNumber() {
    return evaluatorRequestorBridge.getEvaluatorNumber();
  }

  public EvaluatorRequestorBridge getEvaluatorRequestorBridge() {
    return evaluatorRequestorBridge;
  }

  public String getCause() {
    if (jfailedEvaluator.getEvaluatorException() != null) {
      return jfailedEvaluator.getEvaluatorException().getLocalizedMessage();
    }

    return null;
  }

  public String getStackTrace() {
    if (jfailedEvaluator.getEvaluatorException() != null) {
      return ExceptionUtils.getStackTrace(jfailedEvaluator.getEvaluatorException());
    }

    return null;
  }

  public FailedContextBridge[] getFailedContexts() {
    if (jfailedEvaluator.getFailedContextList() == null) {
      return new FailedContextBridge[0];
    }

    final FailedContextBridge[] failedContextBridges =
        new FailedContextBridge[jfailedEvaluator.getFailedContextList().size()];

    for (int i = 0; i < jfailedEvaluator.getFailedContextList().size(); i++) {
      failedContextBridges[i] = new FailedContextBridge(
          jfailedEvaluator.getFailedContextList().get(i), activeContextBridgeFactory);
    }

    return failedContextBridges;
  }

  public FailedTaskBridge getFailedTask() {
    if (!jfailedEvaluator.getFailedTask().isPresent()) {
      return null;
    }

    return new FailedTaskBridge(jfailedEvaluator.getFailedTask().get(), activeContextBridgeFactory);
  }

  @Override
  public void close() {
  }

  @Override
  public String getId() {
    return evaluatorId;
  }
}

