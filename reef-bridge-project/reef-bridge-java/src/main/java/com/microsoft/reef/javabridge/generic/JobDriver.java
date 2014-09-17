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

package com.microsoft.reef.javabridge.generic;

import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.driver.task.*;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.javabridge.*;
import com.microsoft.reef.runtime.common.DriverRestartCompleted;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.reef.util.logging.CLRBufferedLogHandler;
import com.microsoft.reef.webserver.*;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Generic job driver for CLRBridge.
 */
@Unit
public final class JobDriver {

  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());

  private final InteropLogger interopLogger = new InteropLogger();

  private long evaluatorRequestorHandler = 0;
  private long allocatedEvaluatorHandler = 0;
  private long activeContextHandler = 0;
  private long taskMessageHandler = 0;
  private long failedTaskHandler = 0;
  private long failedEvaluatorHandler = 0;
  private long httpServerEventHandler = 0;
  private long completedTaskHandler = 0;
  private long runningTaskHandler = 0;
  private long suspendedTaskHandler = 0;
  private long completedEvaluatorHandler = 0;
  private long closedContextHandler = 0;
  private long failedContextHandler = 0;
  private long contextMessageHandler = 0;
  private long driverRestartHandler = 0;
  private long driverRestartActiveContextHandler = 0;
  private long driverRestartRunningTaskHandler = 0;

  private final NameServer nameServer;
  private final String nameServerInfo;
  private final HttpServer httpServer;

  private int nCLREvaluators = 0;
  private boolean clrBridgeSetup = false;
  private boolean isRestarted = false;

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
  private final EvaluatorRequestor evaluatorRequestor;


  private final DriverStatusManager driverStatusManager;

  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();

  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();

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
            final HttpServer httpServer,
            final NameServer nameServer,
            final JobMessageObserver jobMessageObserver,
            final EvaluatorRequestor evaluatorRequestor,
            final DriverStatusManager driverStatusManager) {
    this.clock = clock;
    this.httpServer = httpServer;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.driverStatusManager = driverStatusManager;
    this.nameServerInfo = NetUtils.getLocalAddress() + ":" + this.nameServer.getPort();
  }

  private void setupBridge(final StartTime startTime)
  {
    // Signal to the clr buffered log handler that the driver has started and that
    // we can begin logging
    LOG.log(Level.INFO, "Initializing CLRBufferedLogHandler...");
    final CLRBufferedLogHandler handler = getCLRBufferedLogHandler();
    if (handler == null) {
      LOG.log(Level.WARNING, "CLRBufferedLogHandler could not be initialized");
    } else {
      handler.setDriverInitialized();
      LOG.log(Level.INFO, "CLRBufferedLogHandler init complete.");
    }

    LOG.log(Level.INFO, "StartTime: {0}", new Object[]{startTime});
    long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
    if (handlers != null) {
      if (handlers.length != NativeInterop.nHandlers) {
        throw new RuntimeException(
            String.format("%s handlers initialized in CLR while native bridge is expecting %s handlers",
                String.valueOf(handlers.length),
                String.valueOf(NativeInterop.nHandlers)));
      }
      this.evaluatorRequestorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.EvaluatorRequestorKey)];
      this.allocatedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.AllocatedEvaluatorKey)];
      this.activeContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ActiveContextKey)];
      this.taskMessageHandler = handlers[NativeInterop.Handlers.get(NativeInterop.TaskMessageKey)];
      this.failedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedTaskKey)];
      this.failedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedEvaluatorKey)];
      this.httpServerEventHandler = handlers[NativeInterop.Handlers.get(NativeInterop.HttpServerKey)];
      this.completedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.CompletedTaskKey)];
      this.runningTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.RunningTaskKey)];
      this.suspendedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.SuspendedTaskKey)];
      this.completedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.CompletedEvaluatorKey)];
      this.closedContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ClosedContextKey)];
      this.failedContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedContextKey)];
      this.contextMessageHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ContextMessageKey)];
      this.driverRestartHandler = handlers[NativeInterop.Handlers.get(NativeInterop.DriverRestartKey)];
      this.driverRestartActiveContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.DriverRestartActiveContextKey)];
      this.driverRestartRunningTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.DriverRestartRunningTaskKey)];
    }

    final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge("SPEC");
    NativeInterop.ClrSystemHttpServerHandlerOnNext(this.httpServerEventHandler, httpServerEventBridge, this.interopLogger);
    final String specList = httpServerEventBridge.getUriSpecification();
    LOG.log(Level.INFO, "Starting http server, getUriSpecification: {0}", specList);
    if (specList != null) {
      final String[] specs = specList.split(":");
      for (final String s : specs) {
        final HttpHandler h = new HttpServerBridgeEventHandler();
        h.setUriSpecification(s);
        this.httpServer.addHttpHandler(h);
      }
    }
    this.clrBridgeSetup = true;
    LOG.log(Level.INFO, "CLR Bridge setup.");
  }

  private CLRBufferedLogHandler getCLRBufferedLogHandler() {
    for (Handler handler : Logger.getLogger("").getHandlers()) {
      if (handler instanceof CLRBufferedLogHandler)
        return (CLRBufferedLogHandler) handler;
    }
    return null;
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
          JobDriver.this.nCLREvaluators--;
        }
      }
    }
  }

  private void submitEvaluator(final AllocatedEvaluator eval, EvaluatorType type) {
    synchronized (JobDriver.this) {
      eval.setType(type);
      LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
          new Object[]{eval.getId(), JobDriver.this.nCLREvaluators, JobDriver.this.contexts.size()});
      if (JobDriver.this.allocatedEvaluatorHandler == 0) {
        throw new RuntimeException("Allocated Evaluator Handler not initialized by CLR.");
      }
      AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(eval, JobDriver.this.nameServerInfo);
      NativeInterop.ClrSystemAllocatedEvaluatorHandlerOnNext(JobDriver.this.allocatedEvaluatorHandler, allocatedEvaluatorBridge, this.interopLogger);
    }
  }

  /**
   * Receive notification that a new Context is available.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "ActiveContextHandler: Context available: {0} expect {1}",
            new Object[]{context.getId(), JobDriver.this.nCLREvaluators});
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
      LOG.log(Level.INFO, "Return results to the client:\n{0}", result);
      JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(result));
      if (JobDriver.this.completedTaskHandler == 0) {
        LOG.log(Level.INFO, "No CLR handler bound to handle completed task.");
      } else {
        LOG.log(Level.INFO, "CLR CompletedTaskHandler handler set, handling things with CLR handler.");
        CompletedTaskBridge completedTaskBridge = new CompletedTaskBridge(task);
        NativeInterop.ClrSystemCompletedTaskHandlerOnNext(JobDriver.this.completedTaskHandler, completedTaskBridge, JobDriver.this.interopLogger);
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
        JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());

        if (failedEvaluatorHandler == 0) {
          if (JobDriver.this.clrBridgeSetup) {
            message = "No CLR FailedEvaluator handler was set, exiting now";
            LOG.log(Level.WARNING, message);
            JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
            return;
          } else {
            clock.scheduleAlarm(0, new EventHandler<Alarm>() {
              @Override
              public void onNext(final Alarm time) {
                if (JobDriver.this.clrBridgeSetup) {
                  handleFailedEvaluatorInCLR(eval);
                } else {
                  LOG.log(Level.INFO, "Waiting for CLR bridge to be set up");
                  clock.scheduleAlarm(5000, this);
                }
              }
            });
          }
        } else {
          handleFailedEvaluatorInCLR(eval);
        }
      }
    }
    private void handleFailedEvaluatorInCLR(final FailedEvaluator eval)
    {
        final String message = "CLR FailedEvaluator handler set, handling things with CLR handler.";
        LOG.log(Level.INFO, message);
        FailedEvaluatorBridge failedEvaluatorBridge = new FailedEvaluatorBridge(eval, JobDriver.this.evaluatorRequestor, JobDriver.this.isRestarted);
        NativeInterop.ClrSystemFailedEvaluatorHandlerOnNext(JobDriver.this.failedEvaluatorHandler, failedEvaluatorBridge, JobDriver.this.interopLogger);
        int additionalRequestedEvaluatorNumber = failedEvaluatorBridge.getNewlyRequestedEvaluatorNumber();
        if (additionalRequestedEvaluatorNumber > 0) {
          nCLREvaluators += additionalRequestedEvaluatorNumber;
          LOG.log(Level.INFO, "number of additional evaluators requested after evaluator failure: " + additionalRequestedEvaluatorNumber);
        }
        JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
    }
  }



  final class HttpServerBridgeEventHandler implements HttpHandler {
    private String uriSpecification;

    public void setUriSpecification(String s) {
      uriSpecification = s;
    }

    /**
     * returns URI specification for the handler
     */
    @Override
    public String getUriSpecification() {
      return uriSpecification;
    }

    /**
     * process http request
     */
    @Override
    public void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response) throws IOException, ServletException {
      LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest: {0}", parsedHttpRequest.getRequestUri());
      final AvroHttpSerializer httpSerializer = new AvroHttpSerializer();
      final AvroHttpRequest avroHttpRequest = httpSerializer.toAvro(parsedHttpRequest);
      final byte[] requestBytes = httpSerializer.toBytes(avroHttpRequest);

      try {
        final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge(requestBytes);
        NativeInterop.ClrSystemHttpServerHandlerOnNext(JobDriver.this.httpServerEventHandler, httpServerEventBridge, JobDriver.this.interopLogger);
        final String responseBody = new String(httpServerEventBridge.getQueryResponseData(), "UTF-8");
        response.getWriter().println(responseBody);
        LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest received response: {0}", responseBody);
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "Fail to invoke CLR Http Server handler", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Handle failed task.
   */
  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws RuntimeException {
      LOG.log(Level.SEVERE, "FailedTask received, will be handle in CLR handler, if set.");
      if (JobDriver.this.failedTaskHandler == 0) {
        LOG.log(Level.SEVERE, "Failed Task Handler not initialized by CLR, fail for real.");
        throw new RuntimeException("Failed Task Handler not initialized by CLR.");
      }
      try {
        FailedTaskBridge failedTaskBridge = new FailedTaskBridge(task);
        NativeInterop.ClrSystemFailedTaskHandlerOnNext(JobDriver.this.failedTaskHandler, failedTaskBridge, JobDriver.this.interopLogger);
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "Fail to invoke CLR failed task handler");
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Receive notification that the Task is running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      if (JobDriver.this.runningTaskHandler == 0) {
        LOG.log(Level.INFO, "RunningTask event received but no CLR handler was bound. Exiting handler.");
      } else {
        LOG.log(Level.INFO, "RunningTask will be handled by CLR handler. Task Id: {0}", task.getId());
        try {
          final RunningTaskBridge runningTaskBridge = new RunningTaskBridge(task);
          NativeInterop.ClrSystemRunningTaskHandlerOnNext(JobDriver.this.runningTaskHandler, runningTaskBridge, JobDriver.this.interopLogger);
        } catch (final Exception ex) {
          LOG.log(Level.WARNING, "Fail to invoke CLR running task handler");
          throw new RuntimeException(ex);
        }
      }
    }
  }

  /**
   * Receive notification that the Task is running when driver restarted.
   */
  final class DriverRestartRunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOG.log(Level.INFO, "DriverRestartRunningTask event received: " + task.getId());
      clock.scheduleAlarm(0, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm time) {
          if (JobDriver.this.clrBridgeSetup)
          {
            if (JobDriver.this.driverRestartRunningTaskHandler != 0) {
              LOG.log(Level.INFO, "CLR driver restart RunningTask handler implemented, now handle it in CLR.");
              NativeInterop.ClrSystemDriverRestartRunningTaskHandlerOnNext(JobDriver.this.driverRestartRunningTaskHandler, new RunningTaskBridge(task));
            }
            else{
              LOG.log(Level.WARNING, "No CLR driver restart RunningTask handler implemented, done with DriverRestartRunningTaskHandler.");
            }
          }
          else
          {
            LOG.log(Level.INFO, "Waiting for driver to complete restart process before checking out CLR driver restart RunningTaskHandler...");
            clock.scheduleAlarm(2000, this);
          }
        }
      });
    }
  }


  /**
   * Receive notification that an context is active on Evaluator when the driver restarted
   */
  final class DriverRestartActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      JobDriver.this.contexts.put(context.getId(), context);
      LOG.log(Level.INFO, "DriverRestartActiveContextHandler event received: " + context.getId());
      clock.scheduleAlarm(0, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm time) {
          if (JobDriver.this.clrBridgeSetup)
          {
            if (JobDriver.this.driverRestartActiveContextHandler != 0) {
              LOG.log(Level.INFO, "CLR driver restart ActiveContext handler implemented, now handle it in CLR.");
              NativeInterop.ClrSystemDriverRestartActiveContextHandlerOnNext(JobDriver.this.driverRestartActiveContextHandler, new ActiveContextBridge(context));
            }
            else{
              LOG.log(Level.WARNING, "No CLR driver restart ActiveContext handler implemented, done with DriverRestartActiveContextHandler.");
            }
          }
          else
          {
            LOG.log(Level.INFO, "Waiting for driver to complete restart process before checking out CLR driver restart DriverRestartActiveContextHandler...");
            clock.scheduleAlarm(2000, this);
          }
        }
      });
    }
  }


  /**
   * Submit a Task to a single Evaluator.
   */
  private void submit(final ActiveContext context) {
    try {
      LOG.log(Level.INFO, "Send task to context: {0}", new Object[]{context});
      if (JobDriver.this.activeContextHandler == 0) {
        throw new RuntimeException("Active Context Handler not initialized by CLR.");
      }
      ActiveContextBridge activeContextBridge = new ActiveContextBridge(context);
      NativeInterop.ClrSystemActiveContextHandlerOnNext(JobDriver.this.activeContextHandler, activeContextBridge, JobDriver.this.interopLogger);
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

        setupBridge(startTime);

        LOG.log(Level.INFO, "Driver Started");

        if (JobDriver.this.evaluatorRequestorHandler == 0) {
          throw new RuntimeException("Evaluator Requestor Handler not initialized by CLR.");
        }
        EvaluatorRequestorBridge evaluatorRequestorBridge = new EvaluatorRequestorBridge(JobDriver.this.evaluatorRequestor, false);
        NativeInterop.ClrSystemEvaluatorRequstorHandlerOnNext(JobDriver.this.evaluatorRequestorHandler, evaluatorRequestorBridge, JobDriver.this.interopLogger);
        // get the evaluator numbers set by CLR handler
        nCLREvaluators += evaluatorRequestorBridge.getEvaluatorNumber();
        LOG.log(Level.INFO, "evaluator requested: " + nCLREvaluators);
      }
    }
  }


  /**
   * Job driver is restarted after previous crash
   */
  final class RestartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      synchronized (JobDriver.this) {

        setupBridge(startTime);

        JobDriver.this.isRestarted = true;

        LOG.log(Level.INFO, "Driver Restarted and CLR bridge set up.");
      }
    }
  }

  /**
   * Receive notification that driver restart has completed.
   */
  final class DriverRestartCompletedHandler implements EventHandler<DriverRestartCompleted> {
    @Override
    public void onNext(final DriverRestartCompleted driverRestartCompleted) {
      LOG.log(Level.INFO, "Java DriverRestartCompleted event received at time [{0}]. ", driverRestartCompleted.getTimeStamp());
      if (JobDriver.this.driverRestartHandler != 0) {
        LOG.log(Level.INFO, "CLR driver restart handler implemented, now handle it in CLR.");
        NativeInterop.ClrSystemDriverRestartHandlerOnNext(JobDriver.this.driverRestartHandler);
      }
      else{
        LOG.log(Level.WARNING, "No CLR driver restart handler implemented, done with DriverRestartCompletedHandler.");
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
      if (JobDriver.this.taskMessageHandler != 0) {
        TaskMessageBridge taskMessageBridge = new TaskMessageBridge(taskMessage);
        // if CLR implements the task message handler, handle the bytes in CLR handler
        NativeInterop.ClrSystemTaskMessageHandlerOnNext(JobDriver.this.taskMessageHandler, taskMessage.get(), taskMessageBridge, JobDriver.this.interopLogger);
      }
    }
  }

  /**
   * Receive notification that the Task has been suspended.
   */
  final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    public final void onNext(final SuspendedTask task) {
      final String message = "Received notification that task [" + task.getId() + "] has been suspended.";
      LOG.log(Level.INFO, message);
      if (JobDriver.this.suspendedTaskHandler != 0) {
        SuspendedTaskBridge suspendedTaskBridge = new SuspendedTaskBridge(task);
        // if CLR implements the suspended task handler, handle it in CLR
        LOG.log(Level.INFO, "Handling the event of suspended task in CLR bridge.");
        NativeInterop.ClrSystemSupendedTaskHandlerOnNext(JobDriver.this.suspendedTaskHandler, suspendedTaskBridge);
      }
      JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(message));
    }
  }

  /**
   * Receive notification that the Evaluator has been shut down.
   */
  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator evaluator) {
      LOG.log(Level.INFO, " Completed Evaluator {0}", evaluator.getId());
      if (JobDriver.this.completedEvaluatorHandler != 0) {
        CompletedEvaluatorBridge completedEvaluatorBridge = new CompletedEvaluatorBridge(evaluator);
        // if CLR implements the completed evaluator handler, handle it in CLR
        LOG.log(Level.INFO, "Handling the event of completed evaluator in CLR bridge.");
        NativeInterop.ClrSystemCompletdEvaluatorHandlerOnNext(completedEvaluatorHandler, completedEvaluatorBridge);
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
      if (JobDriver.this.closedContextHandler != 0) {
        ClosedContextBridge closedContextBridge = new ClosedContextBridge(context);
        // if CLR implements the closed context handler, handle it in CLR
        LOG.log(Level.INFO, "Handling the event of closed context in CLR bridge.");
        NativeInterop.ClrSystemClosedContextHandlerOnNext(JobDriver.this.closedContextHandler, closedContextBridge);
      }
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
      if (JobDriver.this.failedContextHandler != 0) {
        FailedContextBridge failedContextBridge = new FailedContextBridge(context);
        // if CLR implements the failed context handler, handle it in CLR
        LOG.log(Level.INFO, "Handling the event of failed context in CLR bridge.");
        NativeInterop.ClrSystemFailedContextHandlerOnNext(JobDriver.this.failedContextHandler, failedContextBridge);
      }
      synchronized (JobDriver.this) {
        JobDriver.this.contexts.remove(context.getId());
      }
      Optional<byte[]> err = context.getData();
      if (err.isPresent()) {
        JobDriver.this.jobMessageObserver.sendMessageToClient(err.get());
      }
    }
  }


  /**
   * Receive notification that a ContextMessage has been received
   */
  final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage message) {
      LOG.log(Level.SEVERE, "Received ContextMessage:", message.get());
      if (JobDriver.this.contextMessageHandler != 0) {
        ContextMessageBridge contextMessageBridge = new ContextMessageBridge(message);
        // if CLR implements the context message handler, handle it in CLR
        LOG.log(Level.INFO, "Handling the event of context message in CLR bridge.");
        NativeInterop.ClrSystemContextMessageHandlerOnNext(JobDriver.this.contextMessageHandler, contextMessageBridge);
      }
    }
  }

}
