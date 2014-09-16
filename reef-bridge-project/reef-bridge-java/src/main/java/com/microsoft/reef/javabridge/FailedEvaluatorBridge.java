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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;

import java.util.logging.Logger;

public class FailedEvaluatorBridge extends NativeBridge{
  private static final Logger LOG = Logger.getLogger(FailedEvaluatorBridge.class.getName());
  private FailedEvaluator jfailedEvaluator;
  private EvaluatorRequestorBridge evaluatorRequestorBridge;
  private String evaluatorId;

  public FailedEvaluatorBridge(FailedEvaluator failedEvaluator, EvaluatorRequestor evaluatorRequestor, boolean blockedForAdditionalEvaluator)
  {
    jfailedEvaluator = failedEvaluator;
    evaluatorId = failedEvaluator.getId();
    evaluatorRequestorBridge = new EvaluatorRequestorBridge(evaluatorRequestor, blockedForAdditionalEvaluator);
  }

  public int getNewlyRequestedEvaluatorNumber()
  {
    return evaluatorRequestorBridge.getEvaluatorNumber();
  }

  @Override
  public void close()
  {
  }
}

