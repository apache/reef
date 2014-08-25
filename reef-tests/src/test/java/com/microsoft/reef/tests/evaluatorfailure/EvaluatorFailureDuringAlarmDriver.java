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
package com.microsoft.reef.tests.evaluatorfailure;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.tests.TestUtils;
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
final class EvaluatorFailureDuringAlarmDriver {
  private static final Logger LOG = Logger.getLogger(EvaluatorFailureDuringAlarmDriver.class.getName());
  private final AtomicBoolean failedEvaluatorReceived = new AtomicBoolean(false);
  private final AtomicBoolean otherFailuresReceived = new AtomicBoolean(false);

  @Inject
  EvaluatorFailureDuringAlarmDriver() {
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(AllocatedEvaluator allocatedEvaluator) {
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "FailingEvaluator")
          .set(ContextConfiguration.ON_CONTEXT_STARTED, FailureSchedulingContextStartHandler.class)
          .build();
      allocatedEvaluator.submitContext(contextConfiguration);
    }
  }

  final class EvaluatorFailureHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      if (TestUtils.hasCause(failedEvaluator.getEvaluatorException(), ExpectedException.class)) {
        failedEvaluatorReceived.set(true);
        LOG.log(Level.FINEST, "Received an expected exception. All good.");
      } else {
        throw new DriverSideFailure("Received an unexpected exception", failedEvaluator.getEvaluatorException());
      }
    }
  }

  final class ContextFailureHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOG.log(Level.SEVERE, "Received FailedContext: {0}", failedContext);
      otherFailuresReceived.set(true);
    }
  }

  final class TaskFailureHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.log(Level.SEVERE, "Received FailedTask: {0}", failedTask);
      otherFailuresReceived.set(true);

    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      if (failedEvaluatorReceived.get()) {
        LOG.log(Level.FINEST, "Received FailedEvaluator.");
      } else {
        throw new DriverSideFailure("Never Received the FailedEvaluator.");
      }

      if (otherFailuresReceived.get()) {
        throw new DriverSideFailure("Received more events than the FailedEvaluator.");
      }
    }
  }
}
