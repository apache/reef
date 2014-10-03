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
package com.microsoft.reef.tests.fail.driver;

import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.tests.fail.task.FailTaskCall;
import com.microsoft.reef.tests.library.exceptions.SimulatedDriverFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

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

  public final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws SimulatedDriverFailure {
      final SimulatedDriverFailure error = new SimulatedDriverFailure(
          "Simulated Failure at DriverFailOnFail :: " + task.getClass().getName(), task.asError());
      LOG.log(Level.INFO, "Simulated Failure: {0}", error);
      throw error;
    }
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      DriverFailOnFail.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(128).setNumberOfCores(1).build());
    }
  }
}
