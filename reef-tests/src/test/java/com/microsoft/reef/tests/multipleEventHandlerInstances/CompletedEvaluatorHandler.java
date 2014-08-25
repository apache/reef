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
package com.microsoft.reef.tests.multipleEventHandlerInstances;

import com.microsoft.reef.driver.evaluator.CompletedEvaluator;
import com.microsoft.reef.tests.library.exceptions.DriverSideFailure;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class CompletedEvaluatorHandler implements
    EventHandler<CompletedEvaluator> {

  private static final Logger LOG = Logger.getLogger(CompletedEvaluatorHandler.class.getName());

  private static int countInstances = 0;

  @Inject
  public CompletedEvaluatorHandler() {
    ++countInstances;
    if (countInstances > 1) {
      throw new DriverSideFailure("Expect CompletedEvaluatorHandler to be created only once");
    }
  }

  @Override
  public void onNext(CompletedEvaluator completedEvaluator) {
    LOG.log(Level.FINEST, "Received a completed evaluator");
  }

}
