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
package org.apache.reef.tests.yarnschedulingconstraint;


import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.SchedulingConstraint;
import org.apache.reef.driver.evaluator.SchedulingConstraintBuilder;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.hello.HelloTask;
import org.apache.reef.runtime.yarn.driver.evaluator.YarnSchedulingConstraint;
import org.apache.reef.runtime.yarn.driver.evaluator.YarnSchedulingConstraintBuilder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class SchedulingConstraintTestDriver {
  private static final Logger LOG = Logger.getLogger(SchedulingConstraintTestDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final SchedulingConstraintBuilder schedulingConstraintBuilder;

  private final String nodeLabelExpression = "mylabel";
  private int labeledContainerCount;
  private int defaultContainerCount;

  @Inject
  SchedulingConstraintTestDriver(final EvaluatorRequestor evaluatorRequestor,
                                 final SchedulingConstraintBuilder schedulingConstraintBuilder) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.schedulingConstraintBuilder = schedulingConstraintBuilder;
    this.labeledContainerCount = 0;
    this.defaultContainerCount = 0;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      final YarnSchedulingConstraintBuilder yarnSchedulingConstraintBuilder;
      if (schedulingConstraintBuilder instanceof YarnSchedulingConstraintBuilder) {
        yarnSchedulingConstraintBuilder = (YarnSchedulingConstraintBuilder) schedulingConstraintBuilder;
      } else {
        throw new RuntimeException("SchedulingConstraintBuilder is not properly injected!");
      }

      final EvaluatorRequest reqToMylabel = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .setSchedulingConstraint(yarnSchedulingConstraintBuilder
              .requireNodeLabel(nodeLabelExpression)
              .build())
          .build();
      LOG.log(Level.INFO, "Requested Evaluator with node label: {0}",
          SchedulingConstraintTestDriver.this.nodeLabelExpression);
      SchedulingConstraintTestDriver.this.evaluatorRequestor.submit(reqToMylabel);

      final EvaluatorRequest reqToDefault = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build();
      LOG.log(Level.INFO, "Requested Evaluator without node label");
      SchedulingConstraintTestDriver.this.evaluatorRequestor.submit(reqToDefault);
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator Allocated: {0}", allocatedEvaluator);

      final String nodeLabelExpressionFromEvaluator;
      final SchedulingConstraint schedulingConstraint =
          allocatedEvaluator.getEvaluatorDescriptor().getSchedulingConstraint();

      if (schedulingConstraint != null && schedulingConstraint instanceof YarnSchedulingConstraint) {
        nodeLabelExpressionFromEvaluator = (String) schedulingConstraint.get();
      } else {
        nodeLabelExpressionFromEvaluator = null;
      }

      LOG.log(Level.INFO, "Node Labels on this node: {0}", nodeLabelExpressionFromEvaluator);

      if (nodeLabelExpressionFromEvaluator == null) {
        SchedulingConstraintTestDriver.this.defaultContainerCount++;
      } else if (nodeLabelExpressionFromEvaluator.equals("mylabel")) {
        SchedulingConstraintTestDriver.this.labeledContainerCount++;
      }

      LOG.log(Level.INFO, "Submitting HelloREEF task to AllocatedEvaluator: {0}", allocatedEvaluator);

      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.TASK, HelloTask.class)
          .set(TaskConfiguration.IDENTIFIER, "HelloTask")
          .build();

      allocatedEvaluator.submitTask(taskConfiguration);

      allocatedEvaluator.close();
    }
  }

  final class StopHandler implements EventHandler<StopTime> {

    @Override
    public void onNext(final StopTime stopTime) {
      LOG.log(Level.INFO, "# of total default containers: {0}",
          SchedulingConstraintTestDriver.this.defaultContainerCount);
      if (SchedulingConstraintTestDriver.this.defaultContainerCount != 1) {
        throw new RuntimeException("Asked for one default container, but got " +
            SchedulingConstraintTestDriver.this.defaultContainerCount + " default container.");
      }

      LOG.log(Level.INFO, "# of total labeled containers: {0}",
          SchedulingConstraintTestDriver.this.labeledContainerCount);
      if (SchedulingConstraintTestDriver.this.labeledContainerCount != 1) {
        throw new RuntimeException("Asked for one labeled container, but got " +
            SchedulingConstraintTestDriver.this.labeledContainerCount + " labeled container.");
      }
    }
  }
}
