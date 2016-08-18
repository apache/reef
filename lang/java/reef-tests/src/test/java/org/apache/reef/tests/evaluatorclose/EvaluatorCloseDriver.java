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
package org.apache.reef.tests.evaluatorclose;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.driver.task.TaskRepresenter;
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
final class EvaluatorCloseDriver {
  private static final Logger LOG = Logger.getLogger(EvaluatorCloseDriver.class.getName());
  private final AtomicBoolean changedToClosing = new AtomicBoolean(false);
  private TaskRepresenter taskRepresenter;
  private AllocatedEvaluator evaluator;

  @Inject
  EvaluatorCloseDriver() {
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.FINE, "Received a AllocatedEvaluator for Evaluator {0}", allocatedEvaluator.getId());
      evaluator = allocatedEvaluator;
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "EvaluatorCloseTestTask")
          .set(TaskConfiguration.TASK, EvaluatorCloseTestTask.class)
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.FINE, "Received a RunningTask on Evaluator {0}", runningTask.getActiveContext().getEvaluatorId());
      taskRepresenter = runningTask.getTaskRepresenter();
      evaluator.close();
      changedToClosing.set(taskRepresenter.evaluatorIsClosing());
    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      if (!changedToClosing.get()) {
        throw new DriverSideFailure("Evaluator's state was not changed to closing.");
      } else if (!taskRepresenter.evaluatorIsClosed()){
        throw new DriverSideFailure("Evaluator's state was not changed to closed after completion.");
      } else {
        LOG.log(Level.FINEST, "Evaluator state was changed properly (RUNNING -> CLOSING -> CLOSED).");
      }
    }
  }
}
