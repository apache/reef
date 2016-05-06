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
package org.apache.reef.examples.pool;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Allocate N evaluators, submit M tasks to them, and measure the time.
 * Each task does nothing but sleeps for D seconds.
 */
@Unit
public final class JobDriver {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());

  /**
   * Job driver uses EvaluatorRequestor to request Evaluators that will run the Tasks.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * If true, submit context and task in one request.
   */
  private final boolean isPiggyback;

  /**
   * Number of Evaluators to request.
   */
  private final int numEvaluators;

  /**
   * Number of Tasks to run.
   */
  private final int numTasks;
  /**
   * Number of seconds to sleep in each Task.
   * (has to be a String to pass it into Task config).
   */
  private final String delayStr;
  /**
   * Number of Evaluators started.
   */
  private int numEvaluatorsStarted = 0;
  /**
   * Number of Tasks launched.
   */
  private int numTasksStarted = 0;

  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param evaluatorRequestor is used to request Evaluators.
   */
  @Inject
  JobDriver(final EvaluatorRequestor evaluatorRequestor,
            @Parameter(Launch.Piggyback.class) final Boolean isPiggyback,
            @Parameter(Launch.NumEvaluators.class) final Integer numEvaluators,
            @Parameter(Launch.NumTasks.class) final Integer numTasks,
            @Parameter(Launch.Delay.class) final Integer delay) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.isPiggyback = isPiggyback;
    this.numEvaluators = numEvaluators;
    this.numTasks = numTasks;
    this.delayStr = "" + delay;
  }

  /**
   * Build a new Task configuration for a given task ID.
   *
   * @param taskId Unique string ID of the task
   * @return Immutable task configuration object, ready to be submitted to REEF.
   * @throws RuntimeException that wraps BindException if unable to build the configuration.
   */
  private Configuration getTaskConfiguration(final String taskId) {
    try {
      return TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, SleepTask.class)
          .build();
    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Failed to create  Task Configuration: " + taskId, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "TIME: Start Driver with {0} Evaluators", numEvaluators);
      evaluatorRequestor.newRequest()
          .setMemory(128)
          .setNumberOfCores(1)
          .setNumber(numEvaluators)
          .submit();
    }
  }

  /**
   * Job Driver is is shutting down: write to the log.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOG.log(Level.INFO, "TIME: Stop Driver");
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {

      LOG.log(Level.INFO, "TIME: Allocated Evaluator {0}", eval.getId());

      final boolean runTask;
      final int nEval;
      final int nTask;

      synchronized (JobDriver.this) {
        runTask = numTasksStarted < numTasks;
        if (runTask) {
          ++numEvaluatorsStarted;
          if (isPiggyback) {
            ++numTasksStarted;
          }
        }
        nEval = numEvaluatorsStarted;
        nTask = numTasksStarted;
      }

      if (runTask) {

        final String contextId = String.format("Context_%06d", nEval);
        LOG.log(Level.INFO, "TIME: Submit Context {0} to Evaluator {1}",
            new Object[]{contextId, eval.getId()});

        try {

          final JavaConfigurationBuilder contextConfigBuilder =
              Tang.Factory.getTang().newConfigurationBuilder();

          contextConfigBuilder.addConfiguration(ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, contextId)
              .build());

          contextConfigBuilder.bindNamedParameter(Launch.Delay.class, delayStr);

          if (isPiggyback) {

            final String taskId = String.format("StartTask_%08d", nTask);
            final Configuration taskConfig = getTaskConfiguration(taskId);

            LOG.log(Level.INFO, "TIME: Submit Task {0} to Evaluator {1}",
                new Object[]{taskId, eval.getId()});

            eval.submitContextAndTask(contextConfigBuilder.build(), taskConfig);

          } else {
            eval.submitContext(contextConfigBuilder.build());
          }

        } catch (final BindException ex) {
          LOG.log(Level.SEVERE, "Failed to submit Context to Evaluator: " + eval.getId(), ex);
          throw new RuntimeException(ex);
        }
      } else {
        LOG.log(Level.INFO, "TIME: Close Evaluator {0}", eval.getId());
        eval.close();
      }
    }
  }

  /**
   * Receive notification that the Context is active.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {

      LOG.log(Level.INFO, "TIME: Active Context {0}", context.getId());

      if (isPiggyback) {
        return; // Task already submitted
      }

      final boolean runTask;
      final int nTask;

      synchronized (JobDriver.this) {
        runTask = numTasksStarted < numTasks;
        if (runTask) {
          ++numTasksStarted;
        }
        nTask = numTasksStarted;
      }

      if (runTask) {
        final String taskId = String.format("StartTask_%08d", nTask);
        LOG.log(Level.INFO, "TIME: Submit Task {0} to Evaluator {1}",
            new Object[]{taskId, context.getEvaluatorId()});
        context.submitTask(getTaskConfiguration(taskId));
      } else {
        context.close();
      }
    }
  }

  /**
   * Receive notification that the Task is running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOG.log(Level.INFO, "TIME: Running Task {0}", task.getId());
    }
  }

  /**
   * Receive notification that the Task has completed successfully.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {

      final ActiveContext context = task.getActiveContext();
      LOG.log(Level.INFO, "TIME: Completed Task {0} on Evaluator {1}",
          new Object[]{task.getId(), context.getEvaluatorId()});

      final boolean runTask;
      final int nTask;
      synchronized (JobDriver.this) {
        runTask = numTasksStarted < numTasks;
        if (runTask) {
          ++numTasksStarted;
        }
        nTask = numTasksStarted;
      }

      if (runTask) {
        final String taskId = String.format("Task_%08d", nTask);
        LOG.log(Level.INFO, "TIME: Submit Task {0} to Evaluator {1}",
            new Object[]{taskId, context.getEvaluatorId()});
        context.submitTask(getTaskConfiguration(taskId));
      } else {
        LOG.log(Level.INFO, "TIME: Close Evaluator {0}", context.getEvaluatorId());
        context.close();
      }
    }
  }

  /**
   * Receive notification that the Evaluator has been shut down.
   */
  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      LOG.log(Level.INFO, "TIME: Completed Evaluator {0}", eval.getId());
    }
  }
}
