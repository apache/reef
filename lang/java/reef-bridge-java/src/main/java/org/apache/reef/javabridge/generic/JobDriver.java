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
package org.apache.reef.javabridge.generic;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.javabridge.*;
import org.apache.reef.runtime.common.DriverRestartCompleted;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.Optional;
import org.apache.reef.util.logging.CLRBufferedLogHandler;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.webserver.*;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic job driver for CLRBridge.
 */
@Unit
public final class JobDriver {

  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());
  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> JVM_CODEC = new ObjectSerializableCodec<>();
  private final InteropLogger interopLogger = new InteropLogger();
  private final NameServer nameServer;
  private final String nameServerInfo;
  private final HttpServer httpServer;
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

  /**
   * Driver status manager to monitor driver status
   */
  private final DriverStatusManager driverStatusManager;

  /**
   *  NativeInterop has function to load libs when driver starts
   */
  private final LibLoader libLoader;

  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();
  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();

  /**
   * Logging scope factory that provides LoggingScope
   */
  private final LoggingScopeFactory loggingScopeFactory;

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
  private boolean clrBridgeSetup = false;
  private boolean isRestarted = false;

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
            final DriverStatusManager driverStatusManager,
            final LoggingScopeFactory loggingScopeFactory,
            final LibLoader libLoader) {
    this.clock = clock;
    this.httpServer = httpServer;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.driverStatusManager = driverStatusManager;
    this.nameServerInfo = NetUtils.getLocalAddress() + ":" + this.nameServer.getPort();
    this.loggingScopeFactory = loggingScopeFactory;
    this.libLoader = libLoader;
  }

  private void setupBridge(final StartTime startTime) {
    // Signal to the clr buffered log handler that the driver has started and that
    // we can begin logging
    LOG.log(Level.INFO, "Initializing CLRBufferedLogHandler...");
    try (final LoggingScope lb = this.loggingScopeFactory.setupBridge()) {

      try {
        libLoader.loadLib();
      } catch (IOException e) {
        throw new RuntimeException("Fail to load CLR libraries");
      }

      final CLRBufferedLogHandler handler = getCLRBufferedLogHandler();
      if (handler == null) {
        LOG.log(Level.WARNING, "CLRBufferedLogHandler could not be initialized");
      } else {
        handler.setDriverInitialized();
        LOG.log(Level.INFO, "CLRBufferedLogHandler init complete.");
      }

      LOG.log(Level.INFO, "StartTime: {0}", new Object[]{startTime});
      String portNumber = httpServer == null ? null : Integer.toString((httpServer.getPort()));
      long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString(), portNumber);
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

      try (final LoggingScope lp = this.loggingScopeFactory.getNewLoggingScope("setupBridge::ClrSystemHttpServerHandlerOnNext")) {
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
      }
      this.clrBridgeSetup = true;
    }
    LOG.log(Level.INFO, "CLR Bridge setup.");
  }

  private CLRBufferedLogHandler getCLRBufferedLogHandler() {
    for (Handler handler : Logger.getLogger("").getHandlers()) {
      if (handler instanceof CLRBufferedLogHandler)
        return (CLRBufferedLogHandler) handler;
    }
    return null;
  }

  private void submitEvaluator(final AllocatedEvaluator eval, EvaluatorType type) {
    synchronized (JobDriver.this) {
      eval.setType(type);
      LOG.log(Level.INFO, "Allocated Evaluator: {0}, total running running {1}",
          new Object[]{eval.getId(), JobDriver.this.contexts.size()});
      if (JobDriver.this.allocatedEvaluatorHandler == 0) {
        throw new RuntimeException("Allocated Evaluator Handler not initialized by CLR.");
      }
      AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(eval, JobDriver.this.nameServerInfo);
      NativeInterop.ClrSystemAllocatedEvaluatorHandlerOnNext(JobDriver.this.allocatedEvaluatorHandler, allocatedEvaluatorBridge, this.interopLogger);
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
   * Handles AllocatedEvaluator: Submit an empty context
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      try (final LoggingScope ls = loggingScopeFactory.evaluatorAllocated(allocatedEvaluator.getId())) {
        synchronized (JobDriver.this) {
          LOG.log(Level.INFO, "AllocatedEvaluatorHandler.OnNext");
            JobDriver.this.submitEvaluator(allocatedEvaluator, EvaluatorType.CLR);
        }
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      try (final LoggingScope ls = loggingScopeFactory.activeContextReceived(context.getId())) {
        synchronized (JobDriver.this) {
          LOG.log(Level.INFO, "ActiveContextHandler: Context available: {0}",
              new Object[]{context.getId()});
          JobDriver.this.contexts.put(context.getId(), context);
          JobDriver.this.submit(context);
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
      try (final LoggingScope ls = loggingScopeFactory.taskCompleted(task.getId())) {
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
  }

  /**
   * Receive notification that the entire Evaluator had failed.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator eval) {
      try (final LoggingScope ls = loggingScopeFactory.evaluatorFailed(eval.getId())) {
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
    }

    private void handleFailedEvaluatorInCLR(final FailedEvaluator eval) {
      final String message = "CLR FailedEvaluator handler set, handling things with CLR handler.";
      LOG.log(Level.INFO, message);
      FailedEvaluatorBridge failedEvaluatorBridge = new FailedEvaluatorBridge(eval, JobDriver.this.evaluatorRequestor, JobDriver.this.isRestarted, loggingScopeFactory);
      NativeInterop.ClrSystemFailedEvaluatorHandlerOnNext(JobDriver.this.failedEvaluatorHandler, failedEvaluatorBridge, JobDriver.this.interopLogger);
      int additionalRequestedEvaluatorNumber = failedEvaluatorBridge.getNewlyRequestedEvaluatorNumber();
      if (additionalRequestedEvaluatorNumber > 0) {
        LOG.log(Level.INFO, "number of additional evaluators requested after evaluator failure: " + additionalRequestedEvaluatorNumber);
      }
      JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
    }
  }

  final class HttpServerBridgeEventHandler implements HttpHandler {
    private String uriSpecification;

    /**
     * returns URI specification for the handler
     */
    @Override
    public String getUriSpecification() {
      return uriSpecification;
    }

    public void setUriSpecification(String s) {
      uriSpecification = s;
    }

    /**
     * process http request
     */
    @Override
    public void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response) throws IOException, ServletException {
      LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest: {0}", parsedHttpRequest.getRequestUri());
      try (final LoggingScope ls = loggingScopeFactory.httpRequest(parsedHttpRequest.getRequestUri())) {
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
      try (final LoggingScope ls = loggingScopeFactory.taskRunning(task.getId())) {
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
  }

  /**
   * Receive notification that the Task is running when driver restarted.
   */
  final class DriverRestartRunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestartRunningTask(task.getId())) {
        clock.scheduleAlarm(0, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            if (JobDriver.this.clrBridgeSetup) {
              if (JobDriver.this.driverRestartRunningTaskHandler != 0) {
                LOG.log(Level.INFO, "CLR driver restart RunningTask handler implemented, now handle it in CLR.");
                NativeInterop.ClrSystemDriverRestartRunningTaskHandlerOnNext(JobDriver.this.driverRestartRunningTaskHandler, new RunningTaskBridge(task));
              } else {
                LOG.log(Level.WARNING, "No CLR driver restart RunningTask handler implemented, done with DriverRestartRunningTaskHandler.");
              }
            } else {
              LOG.log(Level.INFO, "Waiting for driver to complete restart process before checking out CLR driver restart RunningTaskHandler...");
              clock.scheduleAlarm(2000, this);
            }
          }
        });
      }
    }
  }

  /**
   * Receive notification that an context is active on Evaluator when the driver restarted
   */
  final class DriverRestartActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestartActiveContextReceived(context.getId())) {
        JobDriver.this.contexts.put(context.getId(), context);
      LOG.log(Level.INFO, "DriverRestartActiveContextHandler event received: " + context.getId());
        clock.scheduleAlarm(0, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            if (JobDriver.this.clrBridgeSetup) {
              if (JobDriver.this.driverRestartActiveContextHandler != 0) {
                LOG.log(Level.INFO, "CLR driver restart ActiveContext handler implemented, now handle it in CLR.");
                NativeInterop.ClrSystemDriverRestartActiveContextHandlerOnNext(JobDriver.this.driverRestartActiveContextHandler, new ActiveContextBridge(context));
              } else {
                LOG.log(Level.WARNING, "No CLR driver restart ActiveContext handler implemented, done with DriverRestartActiveContextHandler.");
              }
            } else {
              LOG.log(Level.INFO, "Waiting for driver to complete restart process before checking out CLR driver restart DriverRestartActiveContextHandler...");
              clock.scheduleAlarm(2000, this);
            }
          }
        });
      }
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try (final LoggingScope ls = loggingScopeFactory.driverStart(startTime)) {
        synchronized (JobDriver.this) {

          setupBridge(startTime);

          LOG.log(Level.INFO, "Driver Started");

          if (JobDriver.this.evaluatorRequestorHandler == 0) {
            throw new RuntimeException("Evaluator Requestor Handler not initialized by CLR.");
          }
          EvaluatorRequestorBridge evaluatorRequestorBridge = new EvaluatorRequestorBridge(JobDriver.this.evaluatorRequestor, false, loggingScopeFactory);
          NativeInterop.ClrSystemEvaluatorRequstorHandlerOnNext(JobDriver.this.evaluatorRequestorHandler, evaluatorRequestorBridge, JobDriver.this.interopLogger);
          // get the evaluator numbers set by CLR handler
          LOG.log(Level.INFO, "evaluator requested at start up: " + evaluatorRequestorBridge.getEvaluatorNumber());
        }
      }
    }
  }


  /**
   * Job driver is restarted after previous crash
   */
  final class RestartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestart(startTime)) {
        synchronized (JobDriver.this) {

          setupBridge(startTime);

          JobDriver.this.isRestarted = true;

          LOG.log(Level.INFO, "Driver Restarted and CLR bridge set up.");
        }
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
      try (final LoggingScope ls = loggingScopeFactory.driverRestartCompleted(driverRestartCompleted.getTimeStamp())) {
        if (JobDriver.this.driverRestartHandler != 0) {
          LOG.log(Level.INFO, "CLR driver restart handler implemented, now handle it in CLR.");
          NativeInterop.ClrSystemDriverRestartHandlerOnNext(JobDriver.this.driverRestartHandler);
        } else {
          LOG.log(Level.WARNING, "No CLR driver restart handler implemented, done with DriverRestartCompletedHandler.");

        }
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
      try (final LoggingScope ls = loggingScopeFactory.driverStop(time.getTimeStamp())) {
        for (final ActiveContext context : contexts.values()) {
          context.close();
        }
      }
    }
  }

  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      String msg = new String(taskMessage.get());
      LOG.log(Level.INFO, "Received TaskMessage: {0} from CLR", msg);
      //try (LoggingScope ls = loggingScopeFactory.taskMessageReceived(new String(msg))) {
      if (JobDriver.this.taskMessageHandler != 0) {
        TaskMessageBridge taskMessageBridge = new TaskMessageBridge(taskMessage);
        // if CLR implements the task message handler, handle the bytes in CLR handler
        NativeInterop.ClrSystemTaskMessageHandlerOnNext(JobDriver.this.taskMessageHandler, taskMessage.get(), taskMessageBridge, JobDriver.this.interopLogger);
      }
      //}
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
      try (final LoggingScope ls = loggingScopeFactory.taskSuspended(task.getId())) {
        if (JobDriver.this.suspendedTaskHandler != 0) {
          SuspendedTaskBridge suspendedTaskBridge = new SuspendedTaskBridge(task);
          // if CLR implements the suspended task handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of suspended task in CLR bridge.");
          NativeInterop.ClrSystemSupendedTaskHandlerOnNext(JobDriver.this.suspendedTaskHandler, suspendedTaskBridge);
        }
        JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(message));
      }
    }
  }

  /**
   * Receive notification that the Evaluator has been shut down.
   */
  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator evaluator) {
      LOG.log(Level.INFO, " Completed Evaluator {0}", evaluator.getId());
      try (final LoggingScope ls = loggingScopeFactory.evaluatorCompleted(evaluator.getId())) {
        if (JobDriver.this.completedEvaluatorHandler != 0) {
          CompletedEvaluatorBridge completedEvaluatorBridge = new CompletedEvaluatorBridge(evaluator);
          // if CLR implements the completed evaluator handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of completed evaluator in CLR bridge.");
          NativeInterop.ClrSystemCompletdEvaluatorHandlerOnNext(completedEvaluatorHandler, completedEvaluatorBridge);
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
      try (final LoggingScope ls = loggingScopeFactory.closedContext(context.getId())) {
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
  }


  /**
   * Receive notification that the Context had failed.
   * Remove context from the list of active context and notify the client.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext context) {
      LOG.log(Level.SEVERE, "FailedContext", context);
      try (final LoggingScope ls = loggingScopeFactory.evaluatorFailed(context.getId())) {
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
  }

  /**
   * Receive notification that a ContextMessage has been received
   */
  final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage message) {
      LOG.log(Level.SEVERE, "Received ContextMessage:", message.get());
      try (final LoggingScope ls = loggingScopeFactory.contextMessageReceived(message.get().toString())) {
        if (JobDriver.this.contextMessageHandler != 0) {
          ContextMessageBridge contextMessageBridge = new ContextMessageBridge(message);
          // if CLR implements the context message handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of context message in CLR bridge.");
          NativeInterop.ClrSystemContextMessageHandlerOnNext(JobDriver.this.contextMessageHandler, contextMessageBridge);
        }
      }
    }
  }
}
