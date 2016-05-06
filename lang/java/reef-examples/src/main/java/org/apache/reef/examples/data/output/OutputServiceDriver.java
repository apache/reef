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
package org.apache.reef.examples.data.output;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.data.output.OutputService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the output service demo app.
 */
@Unit
public final class OutputServiceDriver {
  private static final Logger LOG = Logger.getLogger(OutputServiceDriver.class.getName());

  /**
   * Evaluator requestor object used to create new evaluator containers.
   */
  private final EvaluatorRequestor requestor;

  /**
   * Output service object.
   */
  private final OutputService outputService;

  /**
   * Sub-id for Tasks.
   * This object grants different IDs to each task
   * e.g. Task-0, Task-1, and so on.
   */
  private final AtomicInteger taskId = new AtomicInteger(0);

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   * @param outputService output service object.
   */
  @Inject
  public OutputServiceDriver(final EvaluatorRequestor requestor,
                             final OutputService outputService) {
    LOG.log(Level.FINE, "Instantiated 'OutputServiceDriver'");
    this.requestor = requestor;
    this.outputService = outputService;
  }

  /**
   * Handles the StartTime event: Request three Evaluators.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      OutputServiceDriver.this.requestor.newRequest()
          .setNumber(3)
          .setMemory(64)
          .setNumberOfCores(1)
          .submit();
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the output service and a context for it.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Output Service to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "OutputServiceContext")
          .build();
      allocatedEvaluator.submitContextAndService(
          contextConfiguration, outputService.getServiceConfiguration());
    }
  }

  /**
   * Handles ActiveContext: Submit the output service demo task.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO,
          "Submitting OutputServiceREEF task to AllocatedEvaluator: {0}",
          activeContext.getEvaluatorDescriptor());
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "Task-" + taskId.getAndIncrement())
          .set(TaskConfiguration.TASK, OutputServiceTask.class)
          .build();
      activeContext.submitTask(taskConfiguration);
    }
  }
}
