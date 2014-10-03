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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.io.checkpoint.fs.FSCheckPointServiceConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Suspend/resume example job driver. Execute a simple task in all evaluators,
 * and sendEvaluatorControlMessage suspend/resume events properly.
 */
@Unit
public class SuspendDriver {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(SuspendDriver.class.getName());

  /** Number of evaluators to request */
  private static final int NUM_EVALUATORS = 2;

  /**
   * String codec is used to encode the results driver sends to the client.
   */
  private static final ObjectSerializableCodec<String> CODEC_STR = new ObjectSerializableCodec<>();

  /**
   * Integer codec is used to decode the results driver gets from the tasks.
   */
  private static final ObjectSerializableCodec<Integer> CODEC_INT = new ObjectSerializableCodec<>();

  /**
   * Job observer on the client.
   * We use it to send results from the driver back to the client.
   */
  private final JobMessageObserver jobMessageObserver;

  /**
   * Job driver uses EvaluatorRequestor to request Evaluators that will run the Tasks.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * TANG Configuration of the Task.
   */
  private final Configuration contextConfig;

  /**
   * Map from task ID (a string) to the TaskRuntime instance (that can be suspended).
   */
  private final Map<String, RunningTask> runningTasks =
      Collections.synchronizedMap(new HashMap<String, RunningTask>());

  /**
   * Map from task ID (a string) to the SuspendedTask instance (that can be resumed).
   */
  private final Map<String, SuspendedTask> suspendedTasks = new HashMap<>();

  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param evaluatorRequestor is used to request Evaluators.
   * @param numCycles number of cycles to run in the task.
   * @param delay delay in seconds between cycles in the task.
   */
  @Inject
  SuspendDriver(
      final JobMessageObserver jobMessageObserver,
      final EvaluatorRequestor evaluatorRequestor,
      final @Parameter(Launch.Local.class) boolean isLocal,
      final @Parameter(Launch.NumCycles.class) int numCycles,
      final @Parameter(Launch.Delay.class) int delay) {

    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;

    try {

      final Configuration checkpointServiceConfig = FSCheckPointServiceConfiguration.CONF
          .set(FSCheckPointServiceConfiguration.IS_LOCAL, Boolean.toString(isLocal))
          .set(FSCheckPointServiceConfiguration.PATH, "/tmp")
          .set(FSCheckPointServiceConfiguration.PREFIX, "reef-checkpoint-")
          .set(FSCheckPointServiceConfiguration.REPLICATION_FACTOR, "3")
          .build();

      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(Launch.NumCycles.class, Integer.toString(numCycles))
          .bindNamedParameter(Launch.Delay.class, Integer.toString(delay));

      cb.addConfiguration(checkpointServiceConfig);
      this.contextConfig = cb.build();

    } catch (final BindException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Receive notification that the Task is ready to run.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public final void onNext(final RunningTask task) {
      LOG.log(Level.INFO, "Running task: {0}", task.getId());
      runningTasks.put(task.getId(), task);
      jobMessageObserver.sendMessageToClient(CODEC_STR.encode("start task: " + task.getId()));
    }
  }

  /**
   * Receive notification that the Task has completed successfully.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public final void onNext(final CompletedTask task) {

      final EvaluatorDescriptor e = task.getActiveContext().getEvaluatorDescriptor();
      final String msg = "Task completed " + task.getId() + " on node " + e;
      LOG.info(msg);

      jobMessageObserver.sendMessageToClient(CODEC_STR.encode(msg));
      runningTasks.remove(task.getId());
      task.getActiveContext().close();

      final boolean noTasks;

      synchronized (suspendedTasks) {
        LOG.log(Level.INFO, "Tasks running: {0} suspended: {1}", new Object[] {
            runningTasks.size(), suspendedTasks.size() });
        noTasks = runningTasks.isEmpty() && suspendedTasks.isEmpty();
      }

      if (noTasks) {
        LOG.info("All tasks completed; shutting down.");
      }
    }
  }

  /**
   * Receive notification that the Task has been suspended.
   */
  final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    public final void onNext(final SuspendedTask task) {

      final String msg = "Task suspended: " + task.getId();
      LOG.info(msg);

      synchronized (suspendedTasks) {
        suspendedTasks.put(task.getId(), task);
        runningTasks.remove(task.getId());
      }

      jobMessageObserver.sendMessageToClient(CODEC_STR.encode(msg));
    }
  }

  /**
   * Receive message from the Task.
   */
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage message) {
      final int result = CODEC_INT.decode(message.get());
      final String msg = "Task message " + message.getId() + ": " + result;
      LOG.info(msg);
      jobMessageObserver.sendMessageToClient(CODEC_STR.encode(msg));
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      try {

        LOG.log(Level.INFO, "Allocated Evaluator: {0}", eval.getId());

        final Configuration thisContextConfiguration = ContextConfiguration.CONF.set(
            ContextConfiguration.IDENTIFIER, eval.getId() + "_context").build();

        eval.submitContext(Tang.Factory.getTang()
            .newConfigurationBuilder(thisContextConfiguration, contextConfig).build());

      } catch (final BindException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   * Submit a new Task to that Context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public synchronized void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "Active Context: {0}", context.getId());
      try {
        context.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, context.getId() + "_task")
            .set(TaskConfiguration.TASK, SuspendTestTask.class)
            .set(TaskConfiguration.ON_SUSPEND, SuspendTestTask.SuspendHandler.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Bad Task configuration for context: " + context.getId(), ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Handle notifications from the client.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {

      final String commandStr = CODEC_STR.decode(message);
      LOG.log(Level.INFO, "Client message: {0}", commandStr);

      final String[] split = commandStr.split("\\s+", 2);
      if (split.length != 2) {
        throw new IllegalArgumentException("Bad command: " + commandStr);
      } else {

        final String command = split[0].toLowerCase().intern();
        final String taskId = split[1];

        switch (command) {

          case "suspend": {
            final RunningTask task = runningTasks.get(taskId);
            if (task != null) {
              task.suspend();
            } else {
              throw new IllegalArgumentException("Suspend: Task not found: " + taskId);
            }
            break;
          }

          case "resume": {
            final SuspendedTask suspendedTask;
            synchronized (suspendedTasks) {
              suspendedTask = suspendedTasks.remove(taskId);
            }
            if (suspendedTask != null) {
              try {
                suspendedTask.getActiveContext().submitTask(TaskConfiguration.CONF
                    .set(TaskConfiguration.IDENTIFIER, taskId)
                    .set(TaskConfiguration.MEMENTO,
                        DatatypeConverter.printBase64Binary(suspendedTask.get()))
                    .build());
              } catch (final BindException e) {
                throw new RuntimeException(e);
              }
            } else {
              throw new IllegalArgumentException("Resume: Task not found: " + taskId);
            }
            break;
          }

          default:
            throw new IllegalArgumentException("Bad command: " + command);
        }
      }
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setMemory(128).setNumberOfCores(1).setNumber(NUM_EVALUATORS).build());
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime time) {
      LOG.log(Level.INFO, "StopTime: {0}", time);
      jobMessageObserver.sendMessageToClient(CODEC_STR.encode("got StopTime"));
    }
  }
}
