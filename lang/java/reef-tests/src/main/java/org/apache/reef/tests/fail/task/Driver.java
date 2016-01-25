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
package org.apache.reef.tests.fail.task;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Universal driver for the test REEF job that fails on different stages of execution.
 */
@Unit
public final class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());
  private final transient String failTaskName;
  private final transient EvaluatorRequestor requestor;
  private transient String taskId;

  @Inject
  public Driver(@Parameter(FailTaskName.class) final String failTaskName,
                final EvaluatorRequestor requestor) {
    this.failTaskName = failTaskName;
    this.requestor = requestor;
  }

  /**
   * Name of the message class to specify the failing message handler.
   */
  @NamedParameter(doc = "Full name of the (failing) task class", short_name = "task")
  public static final class FailTaskName implements Name<String> {
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {

      try {

        taskId = failTaskName + "_" + eval.getId();
        LOG.log(Level.INFO, "Submit task: {0}", taskId);

        final Configuration contextConfig =
            ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, taskId).build();

        ConfigurationModule taskConfig =
            TaskConfiguration.CONF.set(TaskConfiguration.IDENTIFIER, taskId);

        switch (failTaskName) {
        case "FailTask":
          taskConfig = taskConfig.set(TaskConfiguration.TASK, FailTask.class);
          break;
        case "FailTaskCall":
          taskConfig = taskConfig.set(TaskConfiguration.TASK, FailTaskCall.class);
          break;
        case "FailTaskMsg":
          taskConfig = taskConfig
                .set(TaskConfiguration.TASK, FailTaskMsg.class)
                .set(TaskConfiguration.ON_MESSAGE, FailTaskMsg.class);
          break;
        case "FailTaskSuspend":
          taskConfig = taskConfig
                .set(TaskConfiguration.TASK, FailTaskSuspend.class)
                .set(TaskConfiguration.ON_SUSPEND, FailTaskSuspend.class);
          break;
        case "FailTaskStart":
          taskConfig = taskConfig
                .set(TaskConfiguration.TASK, FailTaskStart.class)
                .set(TaskConfiguration.ON_TASK_STARTED, FailTaskStart.class);
          break;
        case "FailTaskStop":
          taskConfig = taskConfig
                .set(TaskConfiguration.TASK, FailTaskStop.class)
                .set(TaskConfiguration.ON_TASK_STOP, FailTaskStop.class)
                .set(TaskConfiguration.ON_CLOSE, FailTaskStop.CloseEventHandler.class);
          break;
        case "FailTaskClose":
          taskConfig = taskConfig
                .set(TaskConfiguration.TASK, FailTaskClose.class)
                .set(TaskConfiguration.ON_CLOSE, FailTaskClose.class);
          break;
        default:
          break;
        }

        eval.submitContextAndTask(contextConfig, taskConfig.build());

      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Configuration error", ex);
        throw new DriverSideFailure("Configuration error", ex);
      }
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {

      LOG.log(Level.INFO, "TaskRuntime: {0} expect {1}",
          new Object[]{task.getId(), taskId});

      if (!taskId.equals(task.getId())) {
        throw new DriverSideFailure("Task ID " + task.getId()
            + " not equal expected ID " + taskId);
      }

      switch (failTaskName) {
      case "FailTaskMsg":
        LOG.log(Level.INFO, "TaskRuntime: Send message: {0}", task);
        task.send(new byte[0]);
        break;
      case "FailTaskSuspend":
        LOG.log(Level.INFO, "TaskRuntime: Suspend: {0}", task);
        task.suspend();
        break;
      case "FailTaskStop":
      case "FailTaskClose":
        LOG.log(Level.INFO, "TaskRuntime: Stop/Close: {0}", task);
        task.close();
        break;
      default:
        break;
      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) throws DriverSideFailure {
      throw new DriverSideFailure("Unexpected ActiveContext message: " + context.getId());
    }
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      Driver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(128).setNumberOfCores(1).build());
    }
  }
}
