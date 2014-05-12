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
  private long  taskMessageHandler = 0;
  private long  failedTaskHandler = 0;
  private long  failedEvaluatorHandler = 0;

  private int nCLREvaluators = 0;


  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> JVM_CODEC = new ObjectSerializableCodec<>();


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
  private EvaluatorRequestor evaluatorRequestor;

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
    this.evaluatorRequestor = evaluatorRequestor;
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
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "ActiveContextHandler: Context available: {0} expect {1}",
                new Object[]{context.getId(), JobDriver.this.expectCount});
        JobDriver.this.contexts.put(context.getId(), context);
        JobDriver.this.submit(context);
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
          LOG.log(Level.INFO, "Task {0} result {1}: {2}", new Object[]{
                  task.getId(), JobDriver.this.results.size(), result});
          if (--JobDriver.this.expectCount <= 0) {
            JobDriver.this.returnResults();
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
     */
    final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
      @Override
      public void onNext(final FailedEvaluator eval) {
        synchronized (JobDriver.this) {
          LOG.log(Level.SEVERE, "FailedEvaluator", eval);
          for (final FailedContext failedContext : eval.getFailedContextList()) {
            String failedContextId = failedContext.getId();
            LOG.log(Level.INFO, "removing context " + failedContextId + " from job driver contexts.");
            JobDriver.this.contexts.remove(failedContextId);
          }
          String message = "Evaluator " + eval.getId() + " failed with message: "
                  + eval.getEvaluatorException().getMessage();
          JobDriver.this.jobMessageObserver.onNext(message.getBytes());

          if(failedEvaluatorHandler == 0)
          {
            message =  "No CLR FailedEvaluator handler was set, exiting now";
            LOG.log(Level.WARNING, message);
          }
          else
          {
            message =  "CLR FailedEvaluator handler set, handling things with CLR handler.";
            LOG.log(Level.INFO, message);
            InteropLogger interopLogger = new InteropLogger();
            FailedEvaluatorBridge failedEvaluatorBridge = new FailedEvaluatorBridge(eval, JobDriver.this.evaluatorRequestor);
            NativeInterop.ClrSystemFailedEvaluatorHandlerOnNext(failedEvaluatorHandler, failedEvaluatorBridge, interopLogger);

            int additionalRequestedEvaluatorNumber  = failedEvaluatorBridge.getNewlyRequestedEvaluatorNumber();
            if(additionalRequestedEvaluatorNumber > 0)
            {
              nCLREvaluators += additionalRequestedEvaluatorNumber;
              JobDriver.this.expectCount = nCLREvaluators;
              LOG.log(Level.INFO, "number of additional evaluators requested after evaluator failure: " + additionalRequestedEvaluatorNumber);
            }
          }
          JobDriver.this.jobMessageObserver.onNext(message.getBytes());
        }
      }
    }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws RuntimeException {
        LOG.log(Level.SEVERE, "FailedTask received, will be handle in CLR handler, if set.");
        if (activeContextHandler == 0) {
          LOG.log(Level.SEVERE, "Failed Task Handler not initialized by CLR, fail for real.");
          throw new RuntimeException("Failed Task Handler not initialized by CLR.");
        }
        try {
          InteropLogger interopLogger = new InteropLogger();
          FailedTaskBridge failedTaskBridge = new FailedTaskBridge(task);
          NativeInterop.ClrSystemFailedTaskHandlerOnNext(failedTaskHandler, failedTaskBridge, interopLogger);
        } catch (final Exception ex) {
          LOG.log(Level.SEVERE, "Fail to invoke CLR failed task handler");
          throw new RuntimeException(ex);
        }
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
          LOG.log(Level.INFO, "StartTime: {1}", new Object[]{ startTime});
          long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
          if (handlers != null) {
            assert (handlers.length == NativeInterop.nHandlers);
            evaluatorRequestorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.EvaluatorRequestorKey)];
            allocatedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.AllocatedEvaluatorKey)];
            activeContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ActiveContextKey)];
            taskMessageHandler = handlers[NativeInterop.Handlers.get(NativeInterop.TaskMessageKey)];
            failedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedTaskKey)];
            failedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedEvaluatorKey)];
          }

          if (evaluatorRequestorHandler == 0) {
            throw new RuntimeException("Evaluator Requestor Handler not initialized by CLR.");
          }
          EvaluatorRequestorBridge evaluatorRequestorBridge  = new EvaluatorRequestorBridge(JobDriver.this.evaluatorRequestor);
          NativeInterop.ClrSystemEvaluatorRequstorHandlerOnNext(evaluatorRequestorHandler, evaluatorRequestorBridge, interopLogger);
          // get the evaluator numbers set by CLR handler
          nCLREvaluators += evaluatorRequestorBridge.getEvaluatorNumber();
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
        LOG.log(Level.INFO, " StopTime: {0}", new Object[]{time});
        for (final ActiveContext context : contexts.values()) {
          context.close();
        }
      }
    }

    final class TaskMessageHandler implements EventHandler<TaskMessage> {
      @Override
      public void onNext(final TaskMessage taskMessage) {
        LOG.log(Level.INFO, "Received TaskMessage: {0} from CLR", new String(taskMessage.get()));
        if (taskMessageHandler != 0) {
          InteropLogger interopLogger = new InteropLogger();
          TaskMessageBridge taskMessageBridge = new TaskMessageBridge(taskMessage);
          // if CLR implements the task message handler, handle the bytes in CLR handler
          NativeInterop.ClrSystemTaskMessageHandlerOnNext(taskMessageHandler, taskMessage.get(), taskMessageBridge, interopLogger);
        }
      }
    }
}

