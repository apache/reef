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
package com.microsoft.reef.tests.evaluatorexit;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.tests.library.exceptions.DriverSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class EvaluatorExitTestDriver {
  private static final Logger LOG = Logger.getLogger(EvaluatorExitTestDriver.class.getName());
  private final AtomicBoolean failedEvaluatorReceived = new AtomicBoolean(false);

  @Inject
  EvaluatorExitTestDriver() {
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "EvaluatorExitTestTask")
          .set(TaskConfiguration.TASK, EvaluatorExitTestTask.class)
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  final class EvaluatorFailureHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.FINEST, "Received a FailedEvaluator for Evaluator {0}", failedEvaluator.getId());
      failedEvaluatorReceived.set(true);
    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (failedEvaluatorReceived) {
        if (failedEvaluatorReceived.get()) {
          LOG.log(Level.FINE, "Received an expected FailedEvaluator before exit. All good.");
        } else {
          throw new DriverSideFailure("Did not receive an expected FailedEvaluator.");
        }
      }
    }
  }

}
