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
package org.apache.reef.tests.evaluatorexit;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class EvaluatorCompleteTestDriver {
  private static final Logger LOG = Logger.getLogger(EvaluatorCompleteTestDriver.class.getName());
  private final AtomicBoolean completedEvaluatorReceived = new AtomicBoolean(false);
  private final AtomicBoolean completedTaskReceived = new AtomicBoolean(false);

  @Inject
  private EvaluatorCompleteTestDriver() {
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "EvaluatorCompleteTestTask")
          .set(TaskConfiguration.TASK, EvaluatorCompleteTestTask.class)
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  final class EvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {

    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      LOG.log(Level.FINE, "Received a CompletedEvaluator for Evaluator {0}", completedEvaluator.getId());
      completedEvaluatorReceived.set(true);
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.FINE, "Received a CompletedTask for Evaluator {0}", completedTask.getId());
      completedTaskReceived.set(true);
      completedTask.getActiveContext().close();
    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (completedEvaluatorReceived) {
        if (completedEvaluatorReceived.get() && completedTaskReceived.get()) {
          LOG.log(Level.FINE, "Received an expected CompletedEvaluator and CompletedTask before exit. All good.");
        } else {
          throw new DriverSideFailure("Did not receive expected completion events.");
        }
      }
    }
  }

}
