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
package org.apache.reef.tests.fail.driver;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tests.fail.task.FailTaskCall;
import org.apache.reef.tests.library.exceptions.SimulatedDriverFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver which fails on FailedTask event.
 */
@Unit
public final class DriverFailOnFail {

  private static final Logger LOG = Logger.getLogger(DriverFailOnFail.class.getName());

  private final transient JobMessageObserver client;
  private final transient EvaluatorRequestor requestor;

  @Inject
  public DriverFailOnFail(final JobMessageObserver client, final EvaluatorRequestor requestor) {
    this.client = client;
    this.requestor = requestor;
  }

  /**
   * Handler for AllocatedEvaluator.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {

      try {

        LOG.log(Level.INFO, "Submit task: Fail2");

        final Configuration contextConfig = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "Fail2")
            .build();

        final Configuration taskConfig = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "Fail2")
            .set(TaskConfiguration.TASK, FailTaskCall.class)
            .build();

        eval.submitContextAndTask(contextConfig, taskConfig);

      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Handler for FailedTask.
   */
  public final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws SimulatedDriverFailure {
      final SimulatedDriverFailure error = new SimulatedDriverFailure(
          "Simulated Failure at DriverFailOnFail :: " + task.getClass().getName(), task.asError());
      LOG.log(Level.INFO, "Simulated Failure: {0}", error);
      throw error;
    }
  }

  /**
   * Handler for StartTime.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      DriverFailOnFail.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(128).setNumberOfCores(1).build());
    }
  }
}
