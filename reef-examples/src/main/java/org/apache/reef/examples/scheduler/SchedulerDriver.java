/**
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
package org.apache.reef.examples.scheduler;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver for TaskScheduler. It receives the commands by HttpRequest and
 * execute them in a FIFO(First In First Out) order.
 */
@Unit
public final class SchedulerDriver {

  public static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final Logger LOG = Logger.getLogger(SchedulerDriver.class.getName());

  /**
   * Possible states of the job driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du><dd>Initial state. Ready to request an evaluator</dd>
   * <du><code>WAIT_EVALUATORS</code></du><dd>Wait for requested evaluators to be ready.</dd>
   * <du><code>READY</code></du><dd>Wait for the commands. When new Tasks arrive, enqueue the tasks and transit to RUNNING status.</dd>
   * <du><code>RUNNING</code></du><dd>Run commands in the queue. Go back to READY state when the queue is empty.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATOR, READY, RUNNING
  }

  /**
   * If true, it reuses evaluators when Tasks done.
   */
  private boolean retainable;

  private Object lock = new Object();

  @GuardedBy("lock")
  private State state = State.INIT;

  @GuardedBy("lock")
  private final Queue<Pair<Integer, String>> taskQueue;

  @GuardedBy("lock")
  private Integer runningTaskId = null;

  @GuardedBy("lock")
  private Set<Integer> finishedTaskId = new HashSet<>();

  @GuardedBy("lock")
  private Set<Integer> canceledTaskId = new HashSet<>();

  private final EvaluatorRequestor requestor;


  /**
   * Counts how many tasks have been scheduled.
   */
  private AtomicInteger taskCount = new AtomicInteger(0);

  @Inject
  public SchedulerDriver(final EvaluatorRequestor requestor,
                         @Parameter(SchedulerREEF.Retain.class) boolean retainable) {
    this.requestor = requestor;
    this.taskQueue = new LinkedList<>();
    this.retainable = retainable;
  }

  /**
   * The driver is ready to run.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "Driver started at {0}", startTime);
      assert (state == State.INIT);

      requestEvaluator();
    }
  }

  /**
   * Evaluator is allocated. This occurs every time to run commands in Non-retainable version,
   * while occurs only once in the Retainable version
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      LOG.log(Level.INFO, "Evaluator is ready");
      assert (state == State.WAIT_EVALUATOR);

      evaluator.submitContext(ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "SchedulerContext")
        .build());
    }
  }

  /**
   * Now it is ready to schedule tasks. But if the queue is empty,
   * wait until commands coming up.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      synchronized (lock) {
        LOG.log(Level.INFO, "Context available : {0}", context.getId());
        assert (state == State.WAIT_EVALUATOR);

        state = State.READY;
        waitForCommands(context);
      }
    }
  }

  /**
   * Get the list of Tasks. They are classified as their states.
   * @return
   */
  public String getList() {
    synchronized (lock) {
      final StringBuilder sb = new StringBuilder("Running : ");
      if (runningTaskId != null) {
        sb.append(runningTaskId);
      }

      sb.append("\nWaiting :");
      for (final Pair<Integer, String> entity : taskQueue) {
        sb.append(" ").append(entity.first);
      }

      sb.append("\nFinished :");
      for (final int taskIds : finishedTaskId) {
        sb.append(" ").append(taskIds);
      }

      sb.append("\nCanceled :");
      for (final int taskIds : canceledTaskId) {
        sb.append(" ").append(taskIds);
      }
      return sb.toString();
    }
  }

  /**
   * Get the status of a Task.
   * @return
   */
  public String getStatus(final List<String> args) {
    if (args.size() != 1) {
      return getResult(false, "Usage : only one ID at a time");
    }

    final Integer taskId = Integer.valueOf(args.get(0));

    synchronized (lock) {
      if (taskId.equals(runningTaskId)) {
        return getResult(true, "Running");
      } else if (finishedTaskId.contains(taskId)) {
        return getResult(true, "Finished");
      } else if (canceledTaskId.contains(taskId)) {
        return getResult(true, "Canceled");
      }

      for (final Pair<Integer, String> entity : taskQueue) {
        if (taskId == entity.first) {
          return getResult(true, "Waiting");
        }
      }
      return getResult(false, "Not found");
    }
  }

  /**
   * Submit a command to schedule.
   * @return
   */
  public String submitCommands(final List<String> args) {
    if (args.size() != 1) {
      return getResult(false, "Usage : only one ID at a time");
    }

    final String command = args.get(0);

    synchronized (lock) {
      final Integer id = taskCount.incrementAndGet();
      taskQueue.add(new Pair(id, command));

      if (readyToRun()) {
        state = State.RUNNING;
        lock.notify();
      }
      return getResult(true, "Task ID : "+id);
    }
  }

  /**
   * Cancel a Task waiting on the queue. A task cannot be canceled
   * once it is running.
   * @return
   */
  public String cancelTask(final List<String> args) {
    if (args.size() != 1) {
      return getResult(false, "Usage : only one ID at a time");
    }

    final Integer taskId = Integer.valueOf(args.get(0));

    synchronized (lock) {
      if (taskId.equals(runningTaskId)) {
        return getResult(false, "The task is running");
      } else if (finishedTaskId.contains(taskId)) {
        return getResult(false, "Already finished");
      }

      for (final Pair<Integer, String> entity : taskQueue) {
        if (taskId == entity.first) {
          taskQueue.remove(entity);
          canceledTaskId.add(taskId);
          return getResult(true, "Canceled");
        }
      }
      return getResult(false, "Not found");
    }
  }

  /**
   * Clear all the Tasks from the waiting queue.
   * @return
   */
  public String clearList() {
    final int count;
    synchronized (lock) {
      count = taskQueue.size();
      for (Pair<Integer, String> entity : taskQueue) {
        canceledTaskId.add(entity.first);
      }
      taskQueue.clear();
    }
    return getResult(true, count + " tasks removed.");
  }



  /**
   * Non-retainable version of CompletedTaskHandler.
   * When Task completes, it closes the active context to deallocate the evaluator
   * and if there is outstanding commands, allocate another evaluator.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      synchronized (lock) {
        finishedTaskId.add(runningTaskId);
        runningTaskId = null;

        LOG.log(Level.INFO, "Task completed. Reuse the evaluator :", String.valueOf(retainable));

        if (retainable) {
          if (taskQueue.isEmpty()) {
            state = State.READY;
          }
          waitForCommands(task.getActiveContext());
        } else {
          task.getActiveContext().close();
          state = State.WAIT_EVALUATOR;
          requestEvaluator();
        }
      }
    }
  }

  /**
   * Request an evaluator
   */
  private synchronized void requestEvaluator() {
    requestor.submit(EvaluatorRequest.newBuilder()
      .setMemory(128)
      .setNumber(1)
      .build());
  }

  /**
   * @param command The command to execute
   */
  private void submit(final ActiveContext context, final Integer taskId, final String command) {
    final Configuration taskConf = TaskConfiguration.CONF
      .set(TaskConfiguration.TASK, ShellTask.class)
      .set(TaskConfiguration.IDENTIFIER, "ShellTask"+taskId)
      .build();
    final Configuration commandConf = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(Command.class, command)
      .build();

    LOG.log(Level.INFO, "Submitting command : {0}", command);
    final Configuration merged = Configurations.merge(taskConf, commandConf);
    context.submitTask(merged);
  }

  /**
   * Pick up a command from the queue and run it. Wait until
   * any command coming up if no command exists.
   * @param context
   */
  private void waitForCommands(final ActiveContext context) {
    synchronized (lock) {
      while (taskQueue.isEmpty()) {
        // Wait until any commands enter in the queue
        try {
          lock.wait();
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException occurred in SchedulerDriver", e);
        }
      }

      // Run the first command from the queue.
      final Pair<Integer, String> task = taskQueue.poll();
      runningTaskId = task.first;
      final String command = task.second;
      submit(context, runningTaskId, command);
    }
  }

  /**
   * @return {@code true} if it is possible to run commands.
   */
  private boolean readyToRun() {
    synchronized (lock) {
      return state == State.READY && taskQueue.size() > 0;
    }
  }

  /**
   * Return the result including status and message
   * @param success
   * @param message
   * @return
   */
  private static String getResult(final boolean success, final String message) {
    final StringBuilder sb = new StringBuilder();
    final String status = success ? "Success" : "Error";
    return sb.append("[").append(status).append("] ").append(message).toString();
  }
}
