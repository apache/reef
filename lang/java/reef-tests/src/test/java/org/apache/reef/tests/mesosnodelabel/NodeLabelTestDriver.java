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
package org.apache.reef.tests.mesosnodelabel;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.hello.HelloTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class NodeLabelTestDriver {
  private static final Logger LOG = Logger.getLogger(NodeLabelTestDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;

  private final String nodeLabelExpression = "mylabel";
  private int labeledContainerCount;
  private int defaultContainerCount;

  @Inject
  NodeLabelTestDriver(final EvaluatorRequestor evaluatorRequestor) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.labeledContainerCount = 0;
    this.defaultContainerCount = 0;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final EvaluatorRequest reqToMylabel = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .setNodeLabel(NodeLabelTestDriver.this.nodeLabelExpression, NodeLabelTestDriver.this.nodeLabelExpression)
          .build();
      LOG.log(Level.INFO, "Requested Evaluator with node label: {0}", reqToMylabel.getNodeLabels());
      NodeLabelTestDriver.this.evaluatorRequestor.submit(reqToMylabel);

      final EvaluatorRequest reqToDefault = EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build();
      LOG.log(Level.INFO, "Requested Evaluator without node label");
      NodeLabelTestDriver.this.evaluatorRequestor.submit(reqToDefault);
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator Allocated: {0}", allocatedEvaluator);

      final Map<String, String> attributesFromEvaluator =
          allocatedEvaluator.getEvaluatorDescriptor().getNodeLabels();
      LOG.log(Level.INFO, "Node Labels on this node: {0}", attributesFromEvaluator);

      if (attributesFromEvaluator.isEmpty()) {
        NodeLabelTestDriver.this.defaultContainerCount++;
      } else if (attributesFromEvaluator.containsKey("mylabel") && attributesFromEvaluator.containsValue("mylabel")) {
        NodeLabelTestDriver.this.labeledContainerCount++;
      }

      LOG.log(Level.INFO, "Submitting Dummy REEF task to AllocatedEvaluator: {0}", allocatedEvaluator);

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
          NodeLabelTestDriver.this.defaultContainerCount);
      if (NodeLabelTestDriver.this.defaultContainerCount != 1) {
        throw new RuntimeException("Asked for one default container, but got " +
            NodeLabelTestDriver.this.defaultContainerCount + "default container.");
      }

      LOG.log(Level.INFO, "# of total labeled containers: {0}",
          NodeLabelTestDriver.this.labeledContainerCount);
      if (NodeLabelTestDriver.this.labeledContainerCount != 1) {
        throw new RuntimeException("Asked for one labeled container, but got " +
            NodeLabelTestDriver.this.labeledContainerCount + "labeled container.");
      }
    }
  }
}
