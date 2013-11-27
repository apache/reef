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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.io.checkpoint.fs.FSCheckPointServiceConfiguration;
import com.microsoft.reef.util.TANGUtils;
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
import javax.xml.bind.DatatypeConverter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Suspend/resume example job driver. Execute a simple activity in all evaluators,
 * and handle suspend/resume events properly.
 */
@Unit
public class SuspendDriver {
  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(SuspendDriver.class.getName());

  /**
   * String codec is used to encode the results driver sends to the client.
   */
  private static final ObjectSerializableCodec<String> CODEC_STR = new ObjectSerializableCodec<>();

  /**
   * Integer codec is used to decode the results driver gets from the activities.
   */
  private static final ObjectSerializableCodec<Integer> CODEC_INT = new ObjectSerializableCodec<>();

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
   * Job driver uses EvaluatorRequestor to request Evaluators that will run the Activities.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Static catalog of REEF resources.
   * We use it to schedule Activity on every available node.
   */
  private final ResourceCatalog catalog;

  /**
   * TANG Configuration of the Activity.
   */
  private final Configuration contextConfig;

  /**
   * Map from activity ID (a string) to the ActivityRuntime instance (that can be suspended).
   */
  private final Map<String, RunningActivity> runningActivities =
      Collections.synchronizedMap(new HashMap<String, RunningActivity>());

  /**
   * Map from activity ID (a string) to the SuspendedActivity instance (that can be resumed).
   */
  private final Map<String, SuspendedActivity> suspendedActivities = new HashMap<>();

  private final int evaluatorTimeOut = 1000; //ms
  private final int numberOfEvaluatorsRequested = 2;
  private int numberOfEvaluatorsReceived = 0;


  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock              Wake clock to schedule and check up running jobs.
   * @param evaluatorRequestor is used to request Evaluators.
   * @param numCycles          number of cycles to run in the activity.
   * @param delay              delay in seconds between cycles in the activity.
   */
  @Inject
  SuspendDriver(final Clock clock,
                final JobMessageObserver jobMessageObserver,
                final EvaluatorRequestor evaluatorRequestor,
                @Parameter(Launch.Local.class) final boolean isLocal,
                @Parameter(Launch.NumCycles.class) final int numCycles,
                @Parameter(Launch.Delay.class) final int delay) {

    this.clock = clock;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.catalog = evaluatorRequestor.getResourceCatalog();

    try {

      final Configuration checkpointServiceConfig = FSCheckPointServiceConfiguration.CONF
          .set(FSCheckPointServiceConfiguration.IS_LOCAL, Boolean.toString(isLocal))
          .set(FSCheckPointServiceConfiguration.PATH, "/tmp")
          .set(FSCheckPointServiceConfiguration.PREFIX, "reef-checkpoint-")
          .set(FSCheckPointServiceConfiguration.REPLICATION_FACTOR, "3")
          .build();

      final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindNamedParameter(Launch.NumCycles.class, Integer.toString(numCycles));
      cb.bindNamedParameter(Launch.Delay.class, Integer.toString(delay));
      cb.addConfiguration(checkpointServiceConfig);

      this.contextConfig = cb.build();

    } catch (final BindException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Receive notification that the Activity is ready to run.
   */
  final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public final void onNext(final RunningActivity act) {
      LOG.log(Level.INFO, "Running activity: {0}", act.getId());
      SuspendDriver.this.runningActivities.put(act.getId(), act);
      SuspendDriver.this.jobMessageObserver.onNext(CODEC_STR.encode("start activity: " + act.getId()));
    }
  }

  /**
   * Receive notification that the Activity has completed successfully.
   */
  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public final void onNext(final CompletedActivity act) {

      final EvaluatorDescriptor e = act.getActiveContext().getEvaluatorDescriptor();
      final String msg = "Activity completed " + act.getId() + " on node " + e;
      LOG.info(msg);

      SuspendDriver.this.jobMessageObserver.onNext(CODEC_STR.encode(msg));
      SuspendDriver.this.runningActivities.remove(act.getId());
      act.getActiveContext().close();

      final boolean noActivities;
      synchronized (SuspendDriver.this.suspendedActivities) {
        LOG.log(Level.INFO, "Activities running: {0} suspended: {1}", new Object[]{
            SuspendDriver.this.runningActivities.size(),
            SuspendDriver.this.suspendedActivities.size()});
        noActivities = SuspendDriver.this.runningActivities.isEmpty() &&
            SuspendDriver.this.suspendedActivities.isEmpty();
      }

      if (noActivities) {
        LOG.info("All activities completed; shutting down.");
      }
    }
  }

  /**
   * Receive notification that the Activity has been suspended.
   */
  final class SuspendedActivityHandler implements EventHandler<SuspendedActivity> {
    @Override
    public final void onNext(final SuspendedActivity act) {
      final String msg = "Activity suspended: " + act.getId();
      LOG.info(msg);
      synchronized (SuspendDriver.this.suspendedActivities) {
        SuspendDriver.this.suspendedActivities.put(act.getId(), act);
        SuspendDriver.this.runningActivities.remove(act.getId());
      }
      SuspendDriver.this.jobMessageObserver.onNext(CODEC_STR.encode(msg));
    }
  }

  /**
   * Receive message from the Activity.
   */
  final class ActivityMessageHandler implements EventHandler<ActivityMessage> {
    @Override
    public void onNext(final ActivityMessage message) {
      final int result = CODEC_INT.decode(message.get());
      final String msg = "Activity message " + message.getId() + ": " + result;
      LOG.info(msg);
      SuspendDriver.this.jobMessageObserver.onNext(CODEC_STR.encode(msg));
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
        eval.submitContext(TANGUtils.merge(
            SuspendDriver.this.contextConfig,
            ContextConfiguration.CONF.set(
                ContextConfiguration.IDENTIFIER, eval.getId() + "_context").build()));
        ++SuspendDriver.this.numberOfEvaluatorsReceived;
      } catch (final BindException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   * Submit a new Activity to that Context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public synchronized void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "Active Context: {0}", context.getId());
      try {
        context.submitActivity(ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, context.getId() + "_activity")
            .set(ActivityConfiguration.ACTIVITY, SuspendTestActivity.class)
            .set(ActivityConfiguration.ON_SUSPEND, SuspendTestActivity.SuspendHandler.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Bad Activity configuration for context: " + context.getId(), ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Handle notifications from the client.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {

      final String commandStr = CODEC_STR.decode(message);
      LOG.log(Level.INFO, "Client message: {0}", commandStr);

      final String[] split = commandStr.split("\\s+", 2);
      if (split.length != 2) {
        SuspendDriver.this.jobMessageObserver.onError(
            new IllegalArgumentException("Bad command: " + commandStr));
      } else {

        final String command = split[0].toLowerCase().intern();
        final String activityId = split[1];

        switch (command) {

          case "suspend": {
            final RunningActivity activity = SuspendDriver.this.runningActivities.get(activityId);
            if (activity != null) {
              activity.suspend();
            } else {
              SuspendDriver.this.jobMessageObserver.onError(
                  new IllegalArgumentException("Suspend: Activity not found: " + activityId));
            }
            break;
          }

          case "resume": {
            final SuspendedActivity suspendedActivity;
            synchronized (SuspendDriver.this.suspendedActivities) {
              suspendedActivity = SuspendDriver.this.suspendedActivities.remove(activityId);
            }
            if (suspendedActivity != null) {
              try {
                suspendedActivity.getActiveContext().submitActivity(ActivityConfiguration.CONF
                    .set(ActivityConfiguration.IDENTIFIER, activityId)
                    .set(ActivityConfiguration.MEMENTO,
                        DatatypeConverter.printBase64Binary(suspendedActivity.get()))
                    .build());
              } catch (final BindException e) {
                throw new RuntimeException(e);
              }
            } else {
              SuspendDriver.this.jobMessageObserver.onError(
                  new IllegalArgumentException("Resume: Activity not found: " + activityId));
            }
            break;
          }

          default:
            SuspendDriver.this.jobMessageObserver.onError(
                new IllegalArgumentException("Bad command: " + command));
        }
      }
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      SuspendDriver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setSize(EvaluatorRequest.Size.SMALL)
          .setNumber(SuspendDriver.this.numberOfEvaluatorsRequested)
          .build());

      SuspendDriver.this.clock.scheduleAlarm(SuspendDriver.this.evaluatorTimeOut,
          new EventHandler<Alarm>() {
            @Override
            public void onNext(final Alarm alarm) {
              if (SuspendDriver.this.numberOfEvaluatorsRequested >
                  SuspendDriver.this.numberOfEvaluatorsReceived) {
                final String message = "Waited " + SuspendDriver.this.evaluatorTimeOut +
                    "ms for " + SuspendDriver.this.numberOfEvaluatorsRequested +
                    " Evaluators; but only received " +
                    SuspendDriver.this.numberOfEvaluatorsReceived;
                final RuntimeException ex = new RuntimeException(message);
                SuspendDriver.this.jobMessageObserver.onError(ex);
                throw ex;
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
      LOG.log(Level.INFO, "StopTime: {0}", time);
      jobMessageObserver.onNext(CODEC_STR.encode("got StopTime"));
    }
  }
}
