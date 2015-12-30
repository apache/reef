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
package org.apache.reef.examples.hellohttp;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Http Distributed Shell Application.
 */
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
@Unit
public final class HttpShellJobDriver {
  private static final Logger LOG = Logger.getLogger(HttpShellJobDriver.class.getName());

  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  public static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  /**
   * Evaluator Requester.
   */
  private final EvaluatorRequestor evaluatorRequestor;
  /**
   * Number of Evaluators to request (default is 2).
   */
  private final int numEvaluators = 2;
  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();
  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();
  /**
   * Job driver state.
   */
  private State state = State.INIT;
  /**
   * First command to execute. Sometimes client can send us the first command
   * before Evaluators are available; we need to store this command here.
   */
  private String cmd;
  /**
   * Number of evaluators/tasks to complete.
   */
  private int expectCount = 0;
  /**
   * Callback handler for http return message.
   */
  private HttpServerShellCmdHandler.ClientCallBackHandler httpCallbackHandler;

  /**
   * Job Driver Constructor.
   *
   * @param requestor
   * @param clientCallBackHandler
   */
  @Inject
  public HttpShellJobDriver(final EvaluatorRequestor requestor,
                            final HttpServerShellCmdHandler.ClientCallBackHandler clientCallBackHandler) {
    this.evaluatorRequestor = requestor;
    this.httpCallbackHandler = clientCallBackHandler;
    LOG.log(Level.FINE, "Instantiated 'HttpShellJobDriver'");
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
    httpCallbackHandler.onNext(CODEC.encode(sb.toString()));
  }

  /**
   * Submit command to all available evaluators.
   *
   * @param command shell command to execute.
   */
  private synchronized void submit(final String command) {
    LOG.log(Level.INFO, "Submit command {0} to {1} evaluators. state: {2}",
        new Object[]{command, this.contexts.size(), this.state});
    assert this.state == State.READY;
    this.expectCount = this.contexts.size();
    this.state = State.WAIT_TASKS;
    this.cmd = null;
    for (final ActiveContext context : this.contexts.values()) {
      this.submit(context, command);
    }
  }

  /**
   * Submit a Task that execute the command to a single Evaluator.
   * This method is called from <code>submitTask(cmd)</code>.
   */
  private void submit(final ActiveContext context, final String command) {
    try {
      LOG.log(Level.INFO, "Send command {0} to context: {1}", new Object[]{command, context});
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.addConfiguration(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, context.getId() + "_task")
              .set(TaskConfiguration.TASK, ShellTask.class)
              .build()
      );
      cb.bindNamedParameter(Command.class, command);
      context.submitTask(cb.build());
    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Bad Task configuration for context: " + context.getId(), ex);
      context.close();
      throw new RuntimeException(ex);
    }
  }

  /**
   * Request the evaluators.
   */
  private synchronized void requestEvaluators() {
    assert this.state == State.INIT;
    LOG.log(Level.INFO, "Schedule on {0} Evaluators.", this.numEvaluators);
    this.evaluatorRequestor.submit(
        EvaluatorRequest.newBuilder()
            .setMemory(128)
            .setNumberOfCores(1)
            .setNumber(this.numEvaluators).build()
    );
    this.state = State.WAIT_EVALUATORS;
    this.expectCount = this.numEvaluators;
  }

  /**
   * Possible states of the job driver. Can be one of:
   * <dl>
   * <dt><code>INIT</code></dt><dd>initial state, ready to request the evaluators.</dd>
   * <dt><code>WAIT_EVALUATORS</code></dt><dd>Wait for requested evaluators to initialize.</dd>
   * <dt><code>READY</code></dt><dd>Ready to submitTask a new task.</dd>
   * <dt><code>WAIT_TASKS</code></dt><dd>Wait for tasks to complete.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATORS, READY, WAIT_TASKS
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitTask a new Task in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      synchronized (HttpShellJobDriver.this) {
        LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
            new Object[]{eval.getId(), HttpShellJobDriver.this.expectCount, HttpShellJobDriver.this.contexts.size()});
        assert HttpShellJobDriver.this.state == State.WAIT_EVALUATORS;
        try {
          eval.submitContext(ContextConfiguration.CONF.set(
              ContextConfiguration.IDENTIFIER, eval.getId() + "_context").build());
        } catch (final BindException ex) {
          LOG.log(Level.SEVERE, "Failed to submit a context to evaluator: " + eval.getId(), ex);
          throw new RuntimeException(ex);
        }
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
      synchronized (HttpShellJobDriver.this) {
        LOG.log(Level.SEVERE, "FailedEvaluator", eval);
        for (final FailedContext failedContext : eval.getFailedContextList()) {
          HttpShellJobDriver.this.contexts.remove(failedContext.getId());
        }
        throw new RuntimeException("Failed Evaluator: ", eval.getEvaluatorException());
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   * Submit a new Distributed Shell Task to that Context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      synchronized (HttpShellJobDriver.this) {
        LOG.log(Level.INFO, "Context available: {0} expect {1} state {2}",
            new Object[]{context.getId(), HttpShellJobDriver.this.expectCount, HttpShellJobDriver.this.state});
        assert HttpShellJobDriver.this.state == State.WAIT_EVALUATORS;
        HttpShellJobDriver.this.contexts.put(context.getId(), context);
        if (--HttpShellJobDriver.this.expectCount <= 0) {
          HttpShellJobDriver.this.state = State.READY;
          if (HttpShellJobDriver.this.cmd == null) {
            LOG.log(Level.INFO, "All evaluators ready; waiting for command. State: {0}",
                HttpShellJobDriver.this.state);
          } else {
            HttpShellJobDriver.this.submit(HttpShellJobDriver.this.cmd);
          }
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
      synchronized (HttpShellJobDriver.this) {
        HttpShellJobDriver.this.contexts.remove(context.getId());
      }
    }
  }

  final class HttpClientCloseHandler implements EventHandler<Void> {
    @Override
    public void onNext(final Void aVoid) throws RuntimeException {
      LOG.log(Level.INFO, "Received a close message from the client. " +
          "You can put code here to properly close drivers and evaluators.");
      for (final ActiveContext c : contexts.values()) {
        c.close();
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
      synchronized (HttpShellJobDriver.this) {
        HttpShellJobDriver.this.contexts.remove(context.getId());
      }
      throw new RuntimeException("Failed context: ", context.asError());
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
      final String result = CODEC.decode(task.get());
      synchronized (HttpShellJobDriver.this) {
        HttpShellJobDriver.this.results.add(task.getId() + " :: " + result);
        LOG.log(Level.INFO, "Task {0} result {1}: {2} state: {3}", new Object[]{
            task.getId(), HttpShellJobDriver.this.results.size(), result, HttpShellJobDriver.this.state});
        if (--HttpShellJobDriver.this.expectCount <= 0) {
          HttpShellJobDriver.this.returnResults();
          HttpShellJobDriver.this.state = State.READY;
          if (HttpShellJobDriver.this.cmd != null) {
            HttpShellJobDriver.this.submit(HttpShellJobDriver.this.cmd);
          }
        }
      }
    }
  }

  /**
   * Receive notification from the client.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      synchronized (HttpShellJobDriver.this) {
        final String command = CODEC.decode(message);
        LOG.log(Level.INFO, "Client message: {0} state: {1}",
            new Object[]{command, HttpShellJobDriver.this.state});
        assert HttpShellJobDriver.this.cmd == null;
        if (HttpShellJobDriver.this.state == State.READY) {
          HttpShellJobDriver.this.submit(command);
        } else {
          // not ready yet - save the command for better times.
          assert HttpShellJobDriver.this.state == State.WAIT_EVALUATORS;
          HttpShellJobDriver.this.cmd = command;
        }
      }
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      synchronized (HttpShellJobDriver.this) {
        LOG.log(Level.INFO, "{0} StartTime: {1}", new Object[]{state, startTime});
        assert state == State.INIT;
        requestEvaluators();
      }
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime time) {
      synchronized (HttpShellJobDriver.this) {
        LOG.log(Level.INFO, "{0} StopTime: {1}", new Object[]{state, time});
        for (final ActiveContext context : contexts.values()) {
          context.close();
        }
      }
    }
  }
}
