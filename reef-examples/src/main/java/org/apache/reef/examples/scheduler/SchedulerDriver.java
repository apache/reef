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
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.List;
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
   * <du><code>INIT</code></du><dd>Initial state. Ready to request an evaluator.</dd>
   * <du><code>WAIT_EVALUATORS</code></du><dd>Waiting for an evaluator allocated with no active evaluators.</dd>
   * <du><code>READY</code></du><dd>Wait for the commands. Reactivated when a new Task arrives.</dd>
   * <du><code>RUNNING</code></du><dd>Run commands in the queue. Go back to READY state when the queue is empty.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATORS, READY, RUNNING
  }

  /**
   * If true, it reuses evaluators when Tasks done.
   */
  private boolean retainable;

  @GuardedBy("SchedulerDriver.this")
  private State state = State.INIT;

  @GuardedBy("SchedulerDriver.this")
  private Scheduler scheduler;

  @GuardedBy("SchedulerDriver.this")
  private int nMaxEval = 3, nActiveEval = 0, nRequestedEval = 0;

  private final EvaluatorRequestor requestor;

  @Inject
  public SchedulerDriver(final EvaluatorRequestor requestor,
                         @Parameter(SchedulerREEF.Retain.class) boolean retainable,
                         final Scheduler scheduler) {
    this.requestor = requestor;
    this.scheduler = scheduler;
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
      state = State.WAIT_EVALUATORS;

      requestEvaluator(1); // Allocate an initial evaluator to avoid idle state.
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
      synchronized (SchedulerDriver.this) {
        nActiveEval++;
        nRequestedEval--;
      }

      evaluator.submitContext(ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "SchedulerContext")
        .build());
    }
  }

  /**
   * Now it is ready to schedule tasks. But if the queue is empty,
   * wait until commands coming up.
   *
   * If there is no pending task, having more than 1 evaluators must be redundant.
   * It may happen, for example, when tasks are canceled during allocation.
   * In these cases, the new evaluator may be abandoned.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      synchronized (SchedulerDriver.this) {
        LOG.log(Level.INFO, "Context available : {0}", context.getId());

        if (scheduler.hasPendingTasks()) {
          state = State.RUNNING;
          scheduler.submitTask(context);
        } else if (nActiveEval > 1) {
          nActiveEval--;
          context.close();
        } else {
          state = State.READY;
          waitForCommands(context);
        }
      }
    }
  }

  /**
   * Non-retainable version of CompletedTaskHandler.
   * When Task completes, it closes the active context to deallocate the evaluator
   * and if there is outstanding commands, allocate another evaluator.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      final int taskId = Integer.valueOf(task.getId());

      synchronized (SchedulerDriver.this) {
        scheduler.setFinished(taskId);

        LOG.log(Level.INFO, "Task completed. Reuse the evaluator : {0}", String.valueOf(retainable));
        final ActiveContext context = task.getActiveContext();

        if (retainable) {
          retainEvaluator(context);
        } else {
          reallocateEvaluator(context);
        }
      }
    }
  }

  /**
   * Get the list of tasks in the scheduler.
   */
  public synchronized SchedulerResponse getList() {
    return scheduler.getList();
  }

  /**
   * Clear all the Tasks from the waiting queue.
   */
  public synchronized SchedulerResponse clearList() {
    return scheduler.clear();
  }

  /**
   * Get the status of a task.
   */
  public SchedulerResponse getTaskStatus(List<String> args) {
    if (args.size() != 1) {
      return SchedulerResponse.BAD_REQUEST("Usage : only one ID at a time");
    }

    final Integer taskId = Integer.valueOf(args.get(0));

    synchronized (SchedulerDriver.this) {
      return scheduler.getTaskStatus(taskId);
    }
  }

  /**
   * Cancel a Task waiting on the queue. A task cannot be canceled
   * once it is running.
   */
  public SchedulerResponse cancelTask(final List<String> args) {
    if (args.size() != 1) {
      return SchedulerResponse.BAD_REQUEST("Usage : only one ID at a time");
    }

    final Integer taskId = Integer.valueOf(args.get(0));

    synchronized (SchedulerDriver.this) {
      return scheduler.cancelTask(taskId);
    }
  }

  /**
   * Submit a command to schedule.
   */
  public SchedulerResponse submitCommands(final List<String> args) {
    if (args.size() != 1) {
      return SchedulerResponse.BAD_REQUEST("Usage : only one command at a time");
    }

    final String command = args.get(0);
    final Integer id;

    synchronized (SchedulerDriver.this) {
      id = scheduler.assignTaskId();
      scheduler.addTask(new TaskEntity(id, command));

      if (state == State.READY) {
        SchedulerDriver.this.notify(); // Wake up at {waitForCommands}
      } else if (state == State.RUNNING && nMaxEval > nActiveEval + nRequestedEval) {
        requestEvaluator(1);
      }
    }
    return SchedulerResponse.OK("Task ID : " + id);
  }

  /**
   * Update the maximum number of evaluators to hold.
   * Request more evaluators in case there are pending tasks
   * in the queue and the number of evaluators is less than the limit.
   */
  public SchedulerResponse setMaxEvaluators(final List<String> args) {
    if (args.size() != 1) {
      return SchedulerResponse.BAD_REQUEST("Usage : Only one value can be used");
    }

    final int nTarget = Integer.valueOf(args.get(0));

    synchronized (SchedulerDriver.this) {
      if (nTarget < nActiveEval + nRequestedEval) {
        return SchedulerResponse.FORBIDDEN(nActiveEval + nRequestedEval +
          " evaluators are used now. Should be larger than that.");
      }
      nMaxEval = nTarget;

      if (scheduler.hasPendingTasks()) {
        final int nToRequest =
          Math.min(scheduler.getNumPendingTasks(), nMaxEval - nActiveEval) - nRequestedEval;
        requestEvaluator(nToRequest);
      }
      return SchedulerResponse.OK("You can use evaluators up to " + nMaxEval + " evaluators.");
    }
  }

  /**
   * Request evaluators. Passing a non positive number is illegal,
   * so it does not make a trial for that situation.
   */
  private void requestEvaluator(final int numToRequest) {
    if (numToRequest <= 0) {
      throw new IllegalArgumentException("The number of evaluator request should be a positive integer");
    }

    synchronized (SchedulerDriver.this) {
      nRequestedEval += numToRequest;
      requestor.submit(EvaluatorRequest.newBuilder()
        .setMemory(32)
        .setNumber(numToRequest)
        .build());
    }
  }

  /**
   * Pick up a command from the queue and run it. Wait until
   * any command coming up if no command exists.
   */
  private void waitForCommands(final ActiveContext context) {
    synchronized (SchedulerDriver.this) {
      while (!scheduler.hasPendingTasks()) {
        // Wait until any command enters in the queue
        try {
          SchedulerDriver.this.wait();
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException occurred in SchedulerDriver", e);
        }
      }
      // When wakes up, run the first command from the queue.
      state = State.RUNNING;
      scheduler.submitTask(context);
    }
  }

  /**
   * Retain the complete evaluators submitting another task
   * until there is no need to reuse them.
   */
  private synchronized void retainEvaluator(final ActiveContext context) {
    if (scheduler.hasPendingTasks()) {
      scheduler.submitTask(context);
    } else if (nActiveEval > 1) {
      nActiveEval--;
      context.close();
    } else {
      state = State.READY;
      waitForCommands(context);
    }
  }

  /**
   * Always close the complete evaluators and
   * allocate a new evaluator if necessary.
   */
  private synchronized void reallocateEvaluator(final ActiveContext context) {
    nActiveEval--;
    context.close();

    if (scheduler.hasPendingTasks()) {
      requestEvaluator(1);
    } else if (nActiveEval <= 0) {
      state = State.WAIT_EVALUATORS;
      requestEvaluator(1);
    }
  }
}
