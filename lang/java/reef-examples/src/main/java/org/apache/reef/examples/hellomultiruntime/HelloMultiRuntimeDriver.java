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
package org.apache.reef.examples.hellomultiruntime;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.hello.HelloTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application with Multi Runtime.
 */
@Unit
public final class HelloMultiRuntimeDriver {

  private static final Logger LOG = Logger.getLogger(HelloMultiRuntimeDriver.class.getName());

  private final EvaluatorRequestor requestor;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private HelloMultiRuntimeDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'HelloDriver'");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      HelloMultiRuntimeDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .setRuntimeName(org.apache.reef.runtime.local.driver.RuntimeIdentifier.RUNTIME_NAME)
          .build());

      LOG.log(Level.INFO, "Requested Local Evaluator .");

      HelloMultiRuntimeDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
              .setNumber(1)
              .setMemory(64)
              .setNumberOfCores(1)
              .setRuntimeName(org.apache.reef.runtime.yarn.driver.RuntimeIdentifier.RUNTIME_NAME)
              .build());

      LOG.log(Level.INFO, "Requested Yarn Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the HelloTask.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting HelloREEF task to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "HelloREEFTask")
          .set(TaskConfiguration.TASK, HelloTask.class)
          .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }
}
