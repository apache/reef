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
package com.microsoft.reef.examples.ds;

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.*;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Shell job driver. Execute shell command on all evaluators,
 * capture stdout, and return concatenated results back to the client.
 */
@Unit
public final class DistributedShellJobDriver {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(DistributedShellJobDriver.class.getName());

  /**
   * String codec is used to encode the results for passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  /**
   * Wake clock is used to schedule periodical job check-ups.
   */
  private final Clock clock;

  /**
   * Job observer on the client.
   * We use it to send results from the driver back to the DistributedShell client.
   */
  private final JobMessageObserver client;

  /**
   * Job driver uses EvaluatorRequestor to request Evaluators that will run the Activities.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Shell command to execute in every Evaluator/Activity.
   */
  private final String cmd;

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
   * Duration of one clock interval (5 seconds).
   */
  private static final int CHECK_UP_INTERVAL = 5000; // 5 sec.

  /**
   * Distributed Shell job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock Wake clock to schedule and check up running jobs.
   * @param client is used to send messages back to the client.
   * @param evaluatorRequestor is used to request Evaluators.
   * @param cmd shell command to run.
   */
  @Inject
  DistributedShellJobDriver(final Clock clock,
                            final JobMessageObserver client,
                            final EvaluatorRequestor evaluatorRequestor,
                            final @Parameter(DSClient.Command.class) String cmd) {
    this.clock = clock;
    this.client = client;
    this.evaluatorRequestor = evaluatorRequestor;
    this.cmd = cmd;
    this.catalog = evaluatorRequestor.getResourceCatalog();
  }

  /**
   * Receive notification that the Activity has completed successfully.
   * Store the activity results and close the Evaluator.
   */
  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(final CompletedActivity act) {
      LOG.log(Level.INFO, "Completed activity: {0}", act.getId());

      // Take the message returned by the activity and add it to the running result.
      final String result = CODEC.decode(act.get());
      final NodeDescriptor node = act.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor();
      results.add("Node " + node.getName() + ":\n" + result);

      LOG.log(Level.INFO, "Activity result: {0} on node {1}",
              new Object[] { result, node.getName() });

      act.getActiveContext().close();
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitActivity a new Activity in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      try {
        LOG.log(Level.INFO, "Allocated Evaluator: {0}", eval.getId());
        // Submit an Activity that executes the shell command in this Evaluator

        final JavaConfigurationBuilder activityConfigurationBuilder = Tang.Factory.getTang()
            .newConfigurationBuilder();
        activityConfigurationBuilder.bindNamedParameter(
                DSClient.Command.class, DistributedShellJobDriver.this.cmd);
        activityConfigurationBuilder.addConfiguration(
            ActivityConfiguration.CONF
                .set(ActivityConfiguration.IDENTIFIER, eval.getId() + "_shell_activity")
                .set(ActivityConfiguration.ACTIVITY, ShellActivity.class)
                .build());

        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "DS")
            .build();

        eval.submitContextAndActivity(contextConfiguration, activityConfigurationBuilder.build());
      } catch (final BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Event handler that signal the start of execution.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "START TIME: {0}", startTime);
      schedule();
    }
  }

  /**
   * Request evaluators on each node.
   * TODO: Ask for specific nodes. (This is not working in YARN... need to check again at some point.)
   */
  private void schedule() {
    final int numNodes = this.catalog.getNodes().size();
    if (numNodes > 0) {
      LOG.log(Level.INFO, "Schedule on {0} nodes.", numNodes);
      try {
        this.evaluatorRequestor.submit(
            EvaluatorRequest.newBuilder()
                .setSize(EvaluatorRequest.Size.SMALL)
                .setNumber(numNodes).build());
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "submitActivity() failed", ex);
        throw new RuntimeException(ex);
      }
    } else {
      this.clock.scheduleAlarm(CHECK_UP_INTERVAL,
          new EventHandler<Alarm>() {
            @Override
            public void onNext(final Alarm time) {
              LOG.log(Level.INFO, "Alarm: {0}", time);
              schedule();
            }
          });
    }
  }

  /**
   * Event handler signaling the end of the job.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      // Construct the final result and forward it to the Client
      final StringBuilder sb = new StringBuilder();
      for (final String result : DistributedShellJobDriver.this.results) {
        sb.append('\n').append(result);
      }

      client.onNext(CODEC.encode(sb.toString()));
    }
  }
}
