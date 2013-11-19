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
package com.microsoft.reef.examples.retained_eval;

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.contexts.*;
import com.microsoft.reef.driver.evaluator.*;

import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
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

  /**
   * Duration of one clock interval.
   */
  private static final int CHECK_UP_INTERVAL = 1000; // 1 sec.

  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  /**
   * Possible states of the job driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du><dd>initial state, ready to request the evaluators.</dd>
   * <du><code>WAIT_EVALUATORS</code></du><dd>Wait for requested evaluators to initialize.</dd>
   * <du><code>READY</code></du><dd>Ready to submitActivity a new activity.</dd>
   * <du><code>WAIT_ACTIVITIES</code></du><dd>Wait for activities to complete.</dd>
   * </dl>
   */
  private enum State {
    INIT, WAIT_EVALUATORS, READY, WAIT_ACTIVITIES
  }

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
   * to request Evaluators that will run the Activities.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Static catalog of REEF resources.
   * We use it to schedule Activity on every available node.
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

  /**
   * Number of evaluators/activities to complete.
   */
  private int expectCount = 0;

  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock Wake clock to schedule and check up running jobs.
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
    this.catalog = evaluatorRequestor.getResourceCatalog();
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitActivity a new Activity in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "Allocated Evaluator: {0} expect {1} running {2}",
            new Object[] { eval.getId(), JobDriver.this.expectCount, JobDriver.this.contexts.size() });
        assert (JobDriver.this.state == State.WAIT_EVALUATORS);
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
      synchronized (JobDriver.this) {
        LOG.log(Level.SEVERE, "FailedEvaluator", eval);
        for (final FailedContext failedContext : eval.getFailedContextList()) {
          JobDriver.this.contexts.remove(failedContext.getId());
        }
        JobDriver.this.jobMessageObserver.onError(eval.getEvaluatorException());
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   * Submit a new Distributed Shell Activity to that Context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      synchronized (JobDriver.this) {
        LOG.log(Level.INFO, "Context available: {0} expect {1} state {2}",
            new Object[] { context.getId(), JobDriver.this.expectCount, JobDriver.this.state });
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
   * Remove context from the list of active contexts.
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
   * Remove context from the list of active contexts and notify the client.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext context) {
      LOG.log(Level.SEVERE, "FailedContext", context);
      synchronized (JobDriver.this) {
        JobDriver.this.contexts.remove(context.getId());
      }
      JobDriver.this.jobMessageObserver.onError(context.getReason());
    }
  }

  /**
   * Receive notification that the Activity has completed successfully.
   */
  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(final CompletedActivity act) {
      LOG.log(Level.INFO, "Completed activity: {0}", act.getId());
      // Take the message returned by the activity and add it to the running result.
      final String result = CODEC.decode(act.get());
      synchronized (JobDriver.this) {
        JobDriver.this.results.add(act.getId() + " :: " + result);
        LOG.log(Level.INFO, "Activity {0} result {1}: {2} state: {3}", new Object[] {
          act.getId(), JobDriver.this.results.size(), result, JobDriver.this.state });
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
   * Construct the final result and forward it to the JobObserver.
   */
  private void returnResults() {
    final StringBuilder sb = new StringBuilder();
    for (final String result : this.results) {
      sb.append(result);
    }
    this.results.clear();
    LOG.log(Level.INFO, "Return results to the client:\n{0}", sb);
    this.jobMessageObserver.onNext(CODEC.encode(sb.toString()));
  }

  /**
   * Receive notification from the client.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      synchronized (JobDriver.this) {
        final String command = CODEC.decode(message);
        LOG.log(Level.INFO, "Client message: {0} state: {1}",
                new Object[] { command, JobDriver.this.state });
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
   * Submit command to all available evaluators.
   *
   * @param command shell command to execute.
   */
  private void submit(final String command) {
    LOG.log(Level.INFO, "Submit command {0} to {1} evaluators. state: {2}",
        new Object[]{command, this.contexts.size(), this.state});
    assert (this.state == State.READY);
    this.expectCount = this.contexts.size();
    this.state = State.WAIT_ACTIVITIES;
    this.cmd = null;
    for (final ActiveContext context : this.contexts.values()) {
      this.submit(context, command);
    }
  }

  /**
   * Submit an Activity that execute the command to a single Evaluator.
   * This method is called from <code>submitActivity(cmd)</code>.
   */
  private void submit(final ActiveContext context, final String command) {
    try {
      LOG.log(Level.INFO, "Send command {0} to context: {1}", new Object[]{command, context});
      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.addConfiguration(
          ActivityConfiguration.CONF
              .set(ActivityConfiguration.IDENTIFIER, context.getId() + "_activity")
              .set(ActivityConfiguration.ACTIVITY, ShellActivity.class)
              .build());
      cb.bindNamedParameter(Launch.Command.class, command);
      context.submitActivity(cb.build());
    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Bad Activity configuration for context: " + context.getId(), ex);
      this.clock.close();
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
   * Request evaluators on each node.
   * If nodes are not available yet, schedule another request in CHECK_UP_INTERVAL.
   * TODO: Ask for specific nodes. (This is not working in YARN... need to check again at some point.)
   *
   * @throws RuntimeException if any of the requests fails.
   */
  private synchronized void requestEvaluators() {
    assert (this.state == State.INIT);
    final int numNodes = this.catalog.getNodes().size();
    if (numNodes > 0) {
      LOG.log(Level.INFO, "Schedule on {0} nodes.", numNodes);
      this.evaluatorRequestor.submit(
          EvaluatorRequest.newBuilder()
              .setSize(EvaluatorRequest.Size.SMALL)
              .setNumber(numNodes).build());
      this.state = State.WAIT_EVALUATORS;
      this.expectCount = numNodes;
    } else {
      // No nodes available yet - wait and ask again.
      this.clock.scheduleAlarm(CHECK_UP_INTERVAL, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            synchronized (JobDriver.this) {
              LOG.log(Level.INFO, "{0} Alarm: {1}", new Object[] { JobDriver.this.state, time });
              if (JobDriver.this.state == State.INIT) {
                JobDriver.this.requestEvaluators();
              }
            }
          }
        });
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
