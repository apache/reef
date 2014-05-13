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
package com.microsoft.reef.examples.hellohttp;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Http Distributed Shell Application
 */
@Unit
public final class HelloDriver {

    private static final Logger LOG = Logger.getLogger(HelloDriver.class.getName());

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
     * String codec is used to encode the results
     * before passing them back to the client.
     */
    public static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

    /**
     * First command to execute. Sometimes client can send us the first command
     * before Evaluators are available; we need to store this command here.
     */
    private String cmd;

    private final EvaluatorRequestor evaluatorRequestor;

    /**
     * Number of Evalutors to request (default is 1).
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
     * Number of evaluators/tasks to complete.
     */
    private int expectCount = 0;

    /**
     * Callback handler for http return message
     */
    private HttpServeShellCmdtHandler.ClientCallBackHandler httpCallbackHandler;

    /**
     * Job Driver Constructor
     * @param requestor
     * @param clientCallBackHandler
     */
    @Inject
    public HelloDriver(final EvaluatorRequestor requestor, final HttpServeShellCmdtHandler.ClientCallBackHandler clientCallBackHandler) {
        this.evaluatorRequestor = requestor;
        this.httpCallbackHandler = clientCallBackHandler;
        LOG.log(Level.INFO, "Instantiated 'HelloDriver'");
    }

    /**
     * Receive notification that an Evaluator had been allocated,
     * and submitTask a new Task in that Evaluator.
     */
    final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
        @Override
        public void onNext(final AllocatedEvaluator eval) {
            synchronized (HelloDriver.this) {
                LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
                        new Object[]{eval.getId(), HelloDriver.this.expectCount, HelloDriver.this.contexts.size()});
                assert (HelloDriver.this.state == State.WAIT_EVALUATORS);
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
            synchronized (HelloDriver.this) {
                LOG.log(Level.SEVERE, "FailedEvaluator", eval);
                for (final FailedContext failedContext : eval.getFailedContextList()) {
                    HelloDriver.this.contexts.remove(failedContext.getId());
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
            synchronized (HelloDriver.this) {
                LOG.log(Level.INFO, "Context available: {0} expect {1} state {2}",
                        new Object[]{context.getId(), HelloDriver.this.expectCount, HelloDriver.this.state});
                assert (HelloDriver.this.state == State.WAIT_EVALUATORS);
                HelloDriver.this.contexts.put(context.getId(), context);
                if (--HelloDriver.this.expectCount <= 0) {
                    HelloDriver.this.state = State.READY;
                    if (HelloDriver.this.cmd == null) {
                        LOG.log(Level.INFO, "All evaluators ready; waiting for command. State: {0}",
                                HelloDriver.this.state);
                    } else {
                        HelloDriver.this.submit(HelloDriver.this.cmd);
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
            synchronized (HelloDriver.this) {
                HelloDriver.this.contexts.remove(context.getId());
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
            synchronized (HelloDriver.this) {
                HelloDriver.this.contexts.remove(context.getId());
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
            synchronized (HelloDriver.this) {
                HelloDriver.this.results.add(task.getId() + " :: " + result);
                LOG.log(Level.INFO, "Task {0} result {1}: {2} state: {3}", new Object[]{
                        task.getId(), HelloDriver.this.results.size(), result, HelloDriver.this.state});
                if (--HelloDriver.this.expectCount <= 0) {
                    HelloDriver.this.returnResults();
                    HelloDriver.this.state = State.READY;
                    if (HelloDriver.this.cmd != null) {
                        HelloDriver.this.submit(HelloDriver.this.cmd);
                    }
                }
            }
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
        httpCallbackHandler.onNext(CODEC.encode(sb.toString()));
    }

    /**
     * Receive notification from the client.
     */
    final class ClientMessageHandler implements EventHandler<byte[]> {
        @Override
        public void onNext(final byte[] message) {
            synchronized (HelloDriver.this) {
                final String command = CODEC.decode(message);
                LOG.log(Level.INFO, "Client message: {0} state: {1}",
                        new Object[]{command, HelloDriver.this.state});
                assert (HelloDriver.this.cmd == null);
                if (HelloDriver.this.state == State.READY) {
                    HelloDriver.this.submit(command);
                } else {
                    // not ready yet - save the command for better times.
                    assert (HelloDriver.this.state == State.WAIT_EVALUATORS);
                    HelloDriver.this.cmd = command;
                }
            }
        }
    }

    /**
     * Submit command to all available evaluators.
     *
     * @param command shell command to execute.
     */
    private void submit(final String command) {
        LOG.log(Level.INFO, "Submit command {0} to {1} evaluators. state: {2}",
                new Object[]{command, this.contexts.size(), this.state});
        assert (this.state == State.READY);
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
            cb.bindNamedParameter(com.microsoft.reef.examples.hellohttp.ShellTask.Command.class, command);
            context.submitTask(cb.build());
        } catch (final BindException ex) {
            LOG.log(Level.SEVERE, "Bad Task configuration for context: " + context.getId(), ex);
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
            LOG.log(Level.INFO, "{0} StartTime: {1}", new Object[]{state, startTime});
            assert (state == State.INIT);
            requestEvaluators();
        }
    }

    /**
     * Request the evaluators.
     */
    private synchronized void requestEvaluators() {
        assert (this.state == State.INIT);
        LOG.log(Level.INFO, "Schedule on {0} Evaluators.", this.numEvaluators);
        this.evaluatorRequestor.submit(
                EvaluatorRequest.newBuilder()
                        .setMemory(128)
                        .setNumber(this.numEvaluators).build()
        );
        this.state = State.WAIT_EVALUATORS;
        this.expectCount = this.numEvaluators;
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
}
