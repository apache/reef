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
package org.apache.reef.examples.distributedshell;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/** The Driver code for REEF distributed shell application. */
@Unit
public final class ShellDriver {

  private static final Logger LOG = Logger.getLogger(ShellDriver.class.getName());

  private static final Configuration STATIC_TASK_CONFIG = TaskConfiguration.CONF
      .set(TaskConfiguration.IDENTIFIER, "ShellTask")
      .set(TaskConfiguration.TASK, ShellTask.class)
      .build();

  private final EvaluatorRequestor requestor;
  private final int numEvaluators;
  private final String command;

  @Inject
  private ShellDriver(
      final EvaluatorRequestor requestor,
      @Parameter(NumEvaluators.class) final int numEvaluators,
      @Parameter(Command.class) final String command) {

    this.requestor = requestor;
    this.numEvaluators = numEvaluators;
    this.command = command;
  }

  /** Driver start event: Request the evaluators. */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numEvaluators)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
    }
  }

  /** AllocatedEvaluator event: Submit the distributed shell task. */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      LOG.log(Level.INFO,
          "Submitting command {0} task to evaluator: {1}", new Object[] {command, allocatedEvaluator});

      final JavaConfigurationBuilder taskConfigBuilder =
          Tang.Factory.getTang().newConfigurationBuilder(STATIC_TASK_CONFIG);

      taskConfigBuilder.bindNamedParameter(Command.class, command);

      allocatedEvaluator.submitTask(taskConfigBuilder.build());
    }
  }
}
