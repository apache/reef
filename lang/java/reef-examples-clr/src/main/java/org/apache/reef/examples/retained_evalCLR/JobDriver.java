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
package org.apache.reef.examples.retained_evalCLR;

import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.library.Command;
import org.apache.reef.examples.library.ShellTask;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.apache.reef.tang.proto.ClassHierarchyProto;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
  public static final String SHELL_TASK_CLASS_HIERARCHY_FILENAME = "ShellTask.bin";
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());
  /**
   * Duration of one clock interval.
   */
  private static final int CHECK_UP_INTERVAL = 1000; // 1 sec.
  private static final String JVM_CONTEXT_SUFFIX = "_JVMContext";
  private static final String CLR_CONTEXT_SUFFIX = "_CLRContext";
  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> JVM_CODEC = new ObjectSerializableCodec<>();
  public static int totalEvaluators = 2;
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
   * Static catalog of REEF resources.
   * We use it to schedule Task on every available node.
   */
  private final ResourceCatalog catalog;
  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();
  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();
  private int nCLREvaluator = 1;                  // guarded by this
  private int nJVMEvaluator = totalEvaluators - nCLREvaluator;  // guarded by this
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
            final EvaluatorRequestor evaluatorRequestor,
            final ResourceCatalog catalog) {
    this.clock = clock;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.catalog = catalog;
  }

  /**
   * Makes a task configuration for the CLR ShellTask.
   *
   * @param taskId
   * @return task configuration for the CLR Task.
   * @throws BindException
   */
  private static final Configuration getCLRTaskConfiguration(
      final String taskId, final String command) throws BindException {

    final ConfigurationBuilder cb = Tang.Factory.getTang()
        .newConfigurationBuilder(loadShellTaskClassHierarchy(SHELL_TASK_CLASS_HIERARCHY_FILENAME));

    cb.bind("Org.Apache.Reef.Tasks.ITask, Org.Apache.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=69c3241e6f0468ca", "Org.Apache.Reef.Tasks.ShellTask, Org.Apache.Reef.Tasks.ShellTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
    cb.bind("Org.Apache.Reef.Tasks.TaskConfigurationOptions+Identifier, Org.Apache.Reef.Tasks.ITask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=69c3241e6f0468ca", taskId);
    cb.bind("Org.Apache.Reef.Tasks.ShellTask+Command, Org.Apache.Reef.Tasks.ShellTask, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", command);

    return cb.build();
  }

  /**
   * Makes a task configuration for the JVM ShellTask..
   *
   * @param taskId
   * @return task configuration for the JVM Task.
   * @throws BindException
   */
  private static final Configuration getJVMTaskConfiguration(
      final String taskId, final String command) throws BindException {

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.addConfiguration(
        TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, ShellTask.class)
            .build()
    );
    cb.bindNamedParameter(Command.class, command);
    return cb.build();
  }

  /**
   * Loads the class hierarchy.
   *
   * @return
   */
  private static ClassHierarchy loadShellTaskClassHierarchy(String binFile) {
    try (final InputStream chin = new FileInputStream(binFile)) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin);
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy " + binFile;
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
  }

  private void submitEvaluator(final AllocatedEvaluator eval, EvaluatorType type) {
    synchronized (JobDriver.this) {

      String contextIdSuffix = type.equals(EvaluatorType.JVM) ? JVM_CONTEXT_SUFFIX : CLR_CONTEXT_SUFFIX;
      String contextId = eval.getId() + contextIdSuffix;

      eval.setType(type);

      LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
          new Object[]{eval.getId(), JobDriver.this.expectCount, JobDriver.this.contexts.size()});
      assert (JobDriver.this.state == State.WAIT_EVALUATORS);
      try {
        eval.submitContext(ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, contextId).build());
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Failed to submit context " + contextId, ex);
        throw new RuntimeException(ex);
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
      LOG.log(Level.INFO, "Sending command {0} to context: {1}", new Object[]{command, context});
      String taskId = context.getId() + "_task";
      final Configuration taskConfiguration;
      if (context.getId().endsWith(JVM_CONTEXT_SUFFIX)) {
        taskConfiguration = getJVMTaskConfiguration(taskId, command);
      } else {
        taskConfiguration = getCLRTaskConfiguration(taskId, command);
      }
      context.submitTask(taskConfiguration);
    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Bad Task configuration for context: " + context.getId(), ex);
      throw new RuntimeException(ex);
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
   * Request evaluators on each node.
   * If nodes are not available yet, schedule another request in CHECK_UP_INTERVAL.
   * TODO: Ask for specific nodes. (This is not working in YARN... need to check again at some point.)
   *
   * @throws RuntimeException if any of the requests fails.
   */
  private synchronized void requestEvaluators() {
    assert (this.state == State.INIT);
    final int numNodes = totalEvaluators;
    if (numNodes > 0) {
      LOG.log(Level.INFO, "Schedule on {0} nodes.", numNodes);
      this.evaluatorRequestor.submit(
          EvaluatorRequest.newBuilder()
              .setMemory(128)
              .setNumberOfCores(1)
              .setNumber(numNodes).build()
      );
      this.state = State.WAIT_EVALUATORS;
      this.expectCount = numNodes;
    } else {
      // No nodes available yet - wait and ask again.
      this.clock.scheduleAlarm(CHECK_UP_INTERVAL, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm time) {
          synchronized (JobDriver.this) {
            LOG.log(Level.INFO, "{0} Alarm: {1}", new Object[]{JobDriver.this.state, time});
            if (JobDriver.this.state == State.INIT) {
              JobDriver.this.requestEvaluators();
            }
          }
        }
      });
    }
  }

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
   * Handles AllocatedEvaluator: Submit an empty context
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      synchronized (JobDriver.this) {
        if (JobDriver.this.nJVMEvaluator > 0) {
          LOG.log(Level.INFO, "===== adding JVM evaluator =====");
          JobDriver.this.submitEvaluator(allocatedEvaluator, EvaluatorType.JVM);
          JobDriver.this.nJVMEvaluator -= 1;
        } else if (JobDriver.this.nCLREvaluator > 0) {
          LOG.log(Level.INFO, "===== adding CLR evaluator =====");
          JobDriver.this.submitEvaluator(allocatedEvaluator, EvaluatorType.CLR);
          JobDriver.this.nCLREvaluator -= 1;
        }
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
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "Context available: {0} expect {1} state {2}",
            new Object[]{context.getId(), JobDriver.this.expectCount, JobDriver.this.state});
        assert (JobDriver.this.state == State.WAIT_EVALUATORS);
        JobDriver.this.contexts.put(context.getId(), context);
        if (--JobDriver.this.expectCount <= 0) {
          JobDriver.this.state = State.READY;
          if (JobDriver.this.cmd == null) {
            LOG.log(Level.INFO, "All evaluators ready; waiting for command. State: {0}",
                JobDriver.this.state);
          } else {
            JobDriver.this.submit(JobDriver.this.cmd);
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
      throw new RuntimeException("Failed context: ", context.asError());
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
        throw new RuntimeException("Failed Evaluator: ", eval.getEvaluatorException());
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
        if (task.getId().contains(CLR_CONTEXT_SUFFIX)) {
          result = new String(task.get());
        } else {
          result = JVM_CODEC.decode(task.get());
        }
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
          if (JobDriver.this.cmd != null) {
            JobDriver.this.submit(JobDriver.this.cmd);
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
      synchronized (JobDriver.this) {
        final String command = JVM_CODEC.decode(message);
        LOG.log(Level.INFO, "Client message: {0} state: {1}",
            new Object[]{command, JobDriver.this.state});
        assert (JobDriver.this.cmd == null);
        if (JobDriver.this.state == State.READY) {
          JobDriver.this.submit(command);
        } else {
          // not ready yet - save the command for better times.
          assert (JobDriver.this.state == State.WAIT_EVALUATORS);
          JobDriver.this.cmd = command;
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
      LOG.log(Level.INFO, "{0} StartTime: {1}", new Object[]{state, startTime});
      assert (state == State.INIT);
      requestEvaluators();
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
}

