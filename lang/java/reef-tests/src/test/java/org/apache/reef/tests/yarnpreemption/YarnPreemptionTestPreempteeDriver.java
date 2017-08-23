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
package org.apache.reef.tests.yarnpreemption;

import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.junit.Assert;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class YarnPreemptionTestPreempteeDriver {

  private static final Logger LOG = Logger.getLogger(YarnPreemptionTestPreempteeDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private boolean isPreempted;

  @Inject
  private YarnPreemptionTestPreempteeDriver(final EvaluatorRequestor evaluatorRequestor) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.isPreempted = false;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime value) {
      LOG.log(Level.INFO, "Submit an EvaluatorRequest to be preempted");
      YarnPreemptionTestPreempteeDriver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(6)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Start YarnPreemptionTask to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.TASK, YarnPreemptionTask.class)
          .set(TaskConfiguration.IDENTIFIER, "YarnPreemptionTask")
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }

  final class EvaluatorPreemptedHandler implements EventHandler<PreemptedEvaluator> {
    @Override
    public void onNext(final PreemptedEvaluator value) {
      LOG.log(Level.INFO, "Evaluator is preempted!");
      isPreempted = true;
    }
  }

  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator value) {
      LOG.log(Level.INFO, "Evaluator is failed!");
      Assert.assertTrue("Preempt flag: " + isPreempted, isPreempted);
    }
  }
}
