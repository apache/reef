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

package com.microsoft.reef.javabridge.generic;

import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskMessage;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.javabridge.*;
import com.microsoft.reef.util.logging.CLRBufferedLogHandler;
import com.microsoft.reef.webserver.*;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.NetUtils;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import java.io.IOException;
import java.nio.charset.Charset;
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
  private long  httpServerEventHandler = 0;
  private long  completedTaskHandler = 0;
  private long  runningTaskHandler = 0;

  private int nCLREvaluators = 0;

  private NameServer nameServer;
  private String nameServerInfo;
  private HttpServer httpServer;


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
            final HttpServer httpServer,
            final JobMessageObserver jobMessageObserver,
            final EvaluatorRequestor evaluatorRequestor) {
    this.clock = clock;
    this.httpServer = httpServer;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = new NameServer(0, new StringIdentifierFactory());
    this.nameServerInfo = NetUtils.getLocalAddress() + ":" + nameServer.getPort();
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
        AllocatedEvaluatorBridge allocatedEvaluatorBridge = new AllocatedEvaluatorBridge(eval, JobDriver.this.nameServerInfo);
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
        this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(sb.toString()));
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
        LOG.log(Level.INFO, "Return results to the client:\n{0}", result);
        JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(result));
        if(completedTaskHandler == 0)
        {
          LOG.log(Level.INFO, "No CLR handler bound to handle completed task.");
        }
        else
        {
          LOG.log(Level.INFO, "CLR CompletedTaskHandler handler set, handling things with CLR handler.");
          InteropLogger interopLogger = new InteropLogger();
          CompletedTaskBridge completedTaskBridge = new CompletedTaskBridge(task);
          NativeInterop.ClrSystemCompletedTaskHandlerOnNext(completedTaskHandler, completedTaskBridge, interopLogger);
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
              JobDriver.this.jobMessageObserver.sendMessageToClient(err.get());
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
          JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
        }
      }
    }

    final class HttpServerBridgeEventHandler implements HttpHandler {

        private String uriSpecification;

        /**
         * set URI specification
         * @param s
         */
        public void setUriSpecification(String s) {
            uriSpecification = s;
        }

        /**
         * returns URI specification for the handler
         *
         * @return
         */
        @Override
        public String getUriSpecification() {
            return uriSpecification;
        }

        /**
         * it is called when receiving a http request
         *
         * @param request
         * @param response
         */
        @Override
        public void onHttpRequest(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
          LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest is called: {0}", request.getRequestURI());
          final AvroHttpSerializer httpSerializer = new AvroHttpSerializer();
          final AvroHttpRequest avroHttpRequest = httpSerializer.toAvro(request);
          final byte[] requestBytes = httpSerializer.toBytes(avroHttpRequest);

          try {
            final InteropLogger interopLogger = new InteropLogger();
            final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge(requestBytes);
            NativeInterop.ClrSystemHttpServerHandlerOnNext(httpServerEventHandler, httpServerEventBridge, interopLogger);
            final String responseBody = new String(httpServerEventBridge.getQueryResponseData(), "UTF-8");
            response.getWriter().println(responseBody);
            LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest is returned with response: {0}", responseBody);
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
   * Receive notification that the Task is running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {

      if (runningTaskHandler == 0) {
          LOG.log(Level.INFO, "RunningTask event received but no CLR handler was bound. Exiting handler.");
      } else {
          LOG.log(Level.INFO, "RunningTask will be handled by CLR handler. Task Id: {0}", task.getId());
          try {
          final InteropLogger interopLogger = new InteropLogger();
          final RunningTaskBridge runningTaskBridge = new RunningTaskBridge(task);
          NativeInterop.ClrSystemRunningTaskHandlerOnNext(runningTaskHandler, runningTaskBridge, interopLogger);
        } catch (final Exception ex) {
          LOG.log(Level.WARNING, "Fail to invoke CLR running task handler");
          throw new RuntimeException(ex);
        }
      }
    }
  }

    /**
     * Submit a Task to a single Evaluator.
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
          // Signal to the clr buffered log handler that the driver has started and that
          // we can begin logging
          LOG.log(Level.INFO, "Initializing CLRBufferedLogHandler...");
          final CLRBufferedLogHandler handler = getCLRBufferedLogHandler();
          if (handler == null) {
            LOG.log(Level.WARNING, "CLRBufferedLogHandler could not be initialized");
          }
          else {
            handler.setDriverInitialized();
            LOG.log(Level.INFO, "CLRBufferedLogHandler init complete.");
          }

          final InteropLogger interopLogger = new InteropLogger();
          LOG.log(Level.INFO, "StartTime: {0}", new Object[]{ startTime});
          long[] handlers = NativeInterop.CallClrSystemOnStartHandler(startTime.toString());
          if (handlers != null) {
            assert (handlers.length == NativeInterop.nHandlers);
            evaluatorRequestorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.EvaluatorRequestorKey)];
            allocatedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.AllocatedEvaluatorKey)];
            activeContextHandler = handlers[NativeInterop.Handlers.get(NativeInterop.ActiveContextKey)];
            taskMessageHandler = handlers[NativeInterop.Handlers.get(NativeInterop.TaskMessageKey)];
            failedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedTaskKey)];
            failedEvaluatorHandler = handlers[NativeInterop.Handlers.get(NativeInterop.FailedEvaluatorKey)];
            httpServerEventHandler = handlers[NativeInterop.Handlers.get(NativeInterop.HttpServerKey)];
            completedTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.CompletedTaskKey)];
            runningTaskHandler = handlers[NativeInterop.Handlers.get(NativeInterop.RunningTaskKey)];
          }

          final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge("SPEC");
          NativeInterop.ClrSystemHttpServerHandlerOnNext(httpServerEventHandler, httpServerEventBridge, interopLogger);
          final String specList = httpServerEventBridge.getUriSpecification();
          LOG.log(Level.INFO, "StartHandler, getUriSpecification: {0}", specList);
          if (specList != null) {
            final String[] specs = specList.split(":");
            for (final String s : specs) {
              final HttpHandler h = new HttpServerBridgeEventHandler();
              h.setUriSpecification(s);
              httpServer.addHttpHandler(h);
            }
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

      private CLRBufferedLogHandler getCLRBufferedLogHandler() {
        for (Handler handler : Logger.getLogger("").getHandlers()) {
          if (handler instanceof CLRBufferedLogHandler)
            return (CLRBufferedLogHandler) handler;
        }
        return null;
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
