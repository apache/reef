/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.examples.ClrBridge;

import com.microsoft.reef.annotations.Optional;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.TaskMessage;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import javabridge.*;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Retained Evaluator example job driver. Execute shell command on all evaluators,
 * capture stdout, and return concatenated results back to the client.
 */
@Unit
public final class JobDriver {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());

  private long  evaluatorRequestorHandler = 0;
  private long  allocatedEvaluatorHandler = 0;
  private long  activeContextHandler = 0;
  private long  taskMessagetHandler = 0;
  private int nCLREvaluators;
  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> JVM_CODEC = new ObjectSerializableCodec<>();


    /**
   * Possible states of the job driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du><dd>initial state, ready to request the evaluators.</dd>
   * <du><code>WAIT_EVALUATORS</code></du><dd>Wait for requested evaluators to initialize.</dd>
   * <du><code>READY</code></du><dd>Ready to submitTask a new task.</dd>
   * <du><code>WAIT_TASKS</code></du><dd>Wait for tasks to complete.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATORS, READY, WAIT_TASKS
  }

  /**
   * Job driver state.
   */
  private State state = State.INIT;

  /**
   * Wake clock is used to schedule periodical job check-ups.
   */
  private final Clock clock;

  /**
   * Job observer on the client.
   * We use it to send results from the driver back to the client.
   */
  private final JobMessageObserver jobMessageObserver;

  /**
   * Job driver uses EvaluatorRequestor
   * to request Evaluators that will run the Tasks.
   */
  private final EvaluatorRequstorBridge evaluatorRequestorBridge;

  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();

  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();

  /**
   * Number of evaluators/tasks to complete.
   */
  private int expectCount = 0;

  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock              Wake clock to schedule and check up running jobs.
   * @param jobMessageObserver is used to send messages back to the client.
   * @param evaluatorRequestor is used to request Evaluators.
   */
  @Inject
  JobDriver(final Clock clock,
            final JobMessageObserver jobMessageObserver,
            final EvaluatorRequestor evaluatorRequestor) {
    this.clock = clock;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestorBridge = new EvaluatorRequstorBridge(evaluatorRequestor);
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "AllocatedEvaluatorHandler.OnNext");
        if (JobDriver.this.nCLREvaluators > 0) {
          JobDriver.this.submitEvaluator(allocatedEvaluator, EvaluatorType.CLR);
          JobDriver.this.nCLREvaluators -= 1;
        }
      }
    }
  }

  private void submitEvaluator(final AllocatedEvaluator eval, EvaluatorType type) {
    synchronized (JobDriver.this) {
        eval.setType(type);
        LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
                new Object[]{eval.getId(), JobDriver.this.expectCount, JobDriver.this.contexts.size()});
        assert (JobDriver.this.state == State.WAIT_EVALUATORS);
        if(allocatedEvaluatorHandler == 0)
        {
            throw new RuntimeException("Allocated Evaluator Handler not initialized by CLR.");
        }
        InteropLogger interopLogger = new InteropLogger();
        AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(eval);
        NativeInterop.ClrSystemAllocatedEvaluatorHandlerOnNext(allocatedEvaluatorHandler, allocatedEvaluatorBridge,interopLogger);
    }
  }

    /**
     * Construct the final result and forward it to the Client.
     */
    private void returnResults() {
        final StringBuilder sb = new StringBuilder();
        for (final String result : this.results) {
            sb.append(result);
        }
        this.results.clear();
        LOG.log(Level.INFO, "Return results to the client:\n{0}", sb);
        this.jobMessageObserver.onNext(JVM_CODEC.encode(sb.toString()));
    }

  /**
   * Receive notification that a new Context is available.
   * Submit a new Distributed Shell Task to that Context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "ActiveContextHandler: Context available: {0} expect {1} state {2}",
                new Object[]{context.getId(), JobDriver.this.expectCount, JobDriver.this.state});
        assert (JobDriver.this.state == State.WAIT_EVALUATORS);
        JobDriver.this.contexts.put(context.getId(), context);
        if (--JobDriver.this.expectCount <= 0) {
          JobDriver.this.state = State.READY;
          JobDriver.this.expectCount = JobDriver.this.contexts.size();
          for (final ActiveContext activeContext : JobDriver.this.contexts.values()) {
            JobDriver.this.submit(activeContext);
          }
        }
      }
    }
  }

    /**
     * Receive notification that the Task has completed successfully.
     */
    final class CompletedTaskHandler implements EventHandler<CompletedTask> {
      @Override
      public void onNext(final CompletedTask task) {
        LOG.log(Level.INFO, "Completed task: {0}", task.getId());
        // Take the message returned by the task and add it to the running result.
        String result = "default result";
        try {
          result = new String(task.get());
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "failed to decode task outcome");
        }
        synchronized (JobDriver.this) {
          JobDriver.this.results.add(task.getId() + " :: " + result);
          LOG.log(Level.INFO, "Task {0} result {1}: {2} state: {3}", new Object[]{
                  task.getId(), JobDriver.this.results.size(), result, JobDriver.this.state});
          if (--JobDriver.this.expectCount <= 0) {
            JobDriver.this.returnResults();
            JobDriver.this.state = State.READY;
          }
        }
      }
    }

    /**
     * Receive notification that the Context had completed.
     * Remove context from the list of active context.
     */
    final class ClosedContextHandler implements EventHandler<ClosedContext> {
      @Override
      public void onNext(final ClosedContext context) {
        LOG.log(Level.INFO, "Completed Context: {0}", context.getId());
        synchronized (JobDriver.this) {
          JobDriver.this.contexts.remove(context.getId());
        }
      }
    }

    /**
     * Receive notification that the Context had failed.
     * Remove context from the list of active context and notify the client.
     */
    final class FailedContextHandler implements EventHandler<FailedContext> {
      @Override
      public void onNext(final FailedContext context) {
        LOG.log(Level.SEVERE, "FailedContext", context);
        synchronized (JobDriver.this) {
          JobDriver.this.contexts.remove(context.getId());
        }
          com.microsoft.reef.util.Optional<byte[]> err = context.getData();
          if (err.isPresent()) {
              JobDriver.this.jobMessageObserver.onNext(err.get());
          }
      }
    }

    /**
     * Receive notification that the entire Evaluator had failed.
     * Stop other jobs and pass this error to the job observer on the client.
     */
    final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
      @Override
      public void onNext(final FailedEvaluator eval) {
        synchronized (JobDriver.this) {
          LOG.log(Level.SEVERE, "FailedEvaluator", eval);
          for (final FailedContext failedContext : eval.getFailedContextList()) {
            JobDriver.this.contexts.remove(failedContext.getId());
          }
          JobDriver.this.jobMessageObserver.onNext(eval.getEvaluatorException().getMessage().getBytes());
        }
      }
    }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws RuntimeException {
      throw new RuntimeException(String.format("task %s failed with error %s",task.getId(),task.getMessage()));
    }
  }


    /**
     * Submit a Task to a single Evaluator.
     * This method is called from <code>submitTask(cmd)</code>.
     */
    private void submit(final ActiveContext context) {
      try {
        LOG.log(Level.INFO, "Send task to context: {0}", new Object[]{context});
        if (activeContextHandler == 0) {
          throw new RuntimeException("Active Context Handler not initialized by CLR.");
        }
        InteropLogger interopLogger = new InteropLogger();
        ActiveContextBridge activeContextBridge = new ActiveContextBridge(context);
        NativeInterop.ClrSystemActiveContextHandlerOnNext(activeContextHandler, activeContextBridge, interopLogger);
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "Fail to submit task to active context");
        context.close();
        throw new RuntimeException(ex);
      }
    }

    /**
     * Job Driver is ready and the clock is set up: request the evaluators.
     */
    final class StartHandler implements EventHandler<StartTime> {
      @Override
      public void onNext(final StartTime startTime) {
        synchronized (JobDriver.this) {
          InteropLogger interopLogger = new InteropLogger();
          LOG.log(Level.INFO, "{0} StartTime: {1}", new Object[]{state, startTime});
          assert (state == State.INIT);
          long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
          if (handlers != null) {
            assert (handlers.length == NativeInterop.nHandlers);
            evaluatorRequestorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.EvaluatorRequestorKey)];
            allocatedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.AllocatedEvaluatorKey)];
            activeContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ActiveContextKey)];
            taskMessagetHandler = handlers[NativeInterop.Handlers.get(NativeInterop.TaskMessageKey)];
          }

          if (evaluatorRequestorHandler == 0) {
            throw new RuntimeException("Evaluator Requestor Handler not initialized by CLR.");
          }
          NativeInterop.ClrSystemEvaluatorRequstorHandlerOnNext(evaluatorRequestorHandler, evaluatorRequestorBridge, interopLogger);
          // get the evaluator numbers set by CLR handler
          nCLREvaluators = evaluatorRequestorBridge.getEvaluaotrNumber();
          JobDriver.this.state = State.WAIT_EVALUATORS;
          JobDriver.this.expectCount = nCLREvaluators;
          LOG.log(Level.INFO, "evaluator requested: " + nCLREvaluators);
        }
      }
    }

    /**
     * Shutting down the job driver: close the evaluators.
     */
    final class StopHandler implements EventHandler<StopTime> {
      @Override
      public void onNext(final StopTime time) {
        LOG.log(Level.INFO, "{0} StopTime: {1}", new Object[]{state, time});
        for (final ActiveContext context : contexts.values()) {
          context.close();
        }
      }
    }

    final class TaskMessageHandler implements EventHandler<TaskMessage> {
      @Override
      public void onNext(final TaskMessage taskMessage) {
        LOG.log(Level.INFO, "Received TaskMessage: {0} from CLR", new String(taskMessage.get()));
        if (taskMessagetHandler != 0) {
          InteropLogger interopLogger = new InteropLogger();
          TaskMessageBridge taskMessageBridge = new TaskMessageBridge(taskMessage);
          // if CLR implements the task message handler, handle the bytes in CLR handler
          NativeInterop.ClrSystemTaskMessageHandlerOnNext(taskMessagetHandler, taskMessage.get(), taskMessageBridge, interopLogger);
        }
      }
    }
}

