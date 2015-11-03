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

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.util.logging.LoggingScopeFactory;

import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.evaluator.FailedEvaluator}.
 */
@Private
@Interop(CppFiles = { "FailedEvaluatorClr2Java.cpp" }, CsFiles = { "IFailedEvaluatorClr2Java", "FailedEvaluator" })
public final class FailedEvaluatorBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(FailedEvaluatorBridge.class.getName());
  private FailedEvaluator jfailedEvaluator;
  private EvaluatorRequestorBridge evaluatorRequestorBridge;
  private String evaluatorId;

  public FailedEvaluatorBridge(final FailedEvaluator failedEvaluator,
                               final EvaluatorRequestor evaluatorRequestor,
                               final boolean blockedForAdditionalEvaluator,
                               final LoggingScopeFactory loggingScopeFactory) {
    jfailedEvaluator = failedEvaluator;
    evaluatorId = failedEvaluator.getId();
    evaluatorRequestorBridge =
        new EvaluatorRequestorBridge(evaluatorRequestor, blockedForAdditionalEvaluator, loggingScopeFactory);
  }

  public int getNewlyRequestedEvaluatorNumber() {
    return evaluatorRequestorBridge.getEvaluatorNumber();
  }

  @Override
  public void close() {
  }
}

