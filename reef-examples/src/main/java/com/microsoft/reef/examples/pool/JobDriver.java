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
package com.microsoft.reef.examples.pool;

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.context.*;
import com.microsoft.reef.driver.evaluator.*;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Allocate N evaluators, submit M activities to them, and measure the time.
 * Each activity does nothing but sleeps for D seconds.
 */
@Unit
public final class JobDriver {

  /**
   * Standard Java logger.
   */
  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());

  /**
   * Job driver uses EvaluatorRequestor
   * to request Evaluators that will run the Activities.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /** Number of Evaluators to request. */
  private final int numEvaluators;

  /** Number of Activities to run. */
  private final int numActivities;

  /** Number of Evaluators started. */
  private int numEvaluatorsStarted = 0;

  /** Number of Activities launched. */
  private int numActivitiesStarted = 0;

  /**
   * Number of seconds to sleep in each Activity.
   * (has to be a String to pass it into Activity config).
   */
  private final String delayStr;

  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param evaluatorRequestor is used to request Evaluators.
   */
  @Inject
  JobDriver(final EvaluatorRequestor evaluatorRequestor,
            final @Parameter(Launch.NumEvaluators.class) Integer numEvaluators,
            final @Parameter(Launch.NumActivities.class) Integer numActivities,
            final @Parameter(Launch.Delay.class) Integer delay) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.numEvaluators = numEvaluators;
    this.numActivities = numActivities;
    this.delayStr = "" + delay;
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "TIME: Start Driver with {0} Evaluators", numEvaluators);
      evaluatorRequestor.submit(
          EvaluatorRequest.newBuilder()
              .setSize(EvaluatorRequest.Size.SMALL)
              .setNumber(numEvaluators).build());
    }
  }

  /**
   * Job Driver is is shutting down: write to the log.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOG.log(Level.INFO, "TIME: Stop Driver");
    }
  }

  /**
   * Receive notification that an Evaluator had been allocated,
   * and submitActivity a new Activity in that Evaluator.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.INFO, "TIME: Allocated Evaluator {0}", eval.getId());
      synchronized (JobDriver.this) {
        if (numActivitiesStarted < numActivities) {
          ++numActivitiesStarted;
          ++numEvaluatorsStarted;
          final String activityId = String.format("StartActivity_%08d", numActivitiesStarted);
          LOG.log(Level.INFO, "TIME: Submit Activity {0} to Evaluator {1}",
                  new Object[] { activityId, eval.getId() });
          try {
            final JavaConfigurationBuilder contextConfigBuilder =
                Tang.Factory.getTang().newConfigurationBuilder();
            contextConfigBuilder.addConfiguration(ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, String.format("Context_%06d", numEvaluatorsStarted))
                .build());
            contextConfigBuilder.bindNamedParameter(Launch.Delay.class, delayStr);
            final Configuration activityConfig = ActivityConfiguration.CONF
                .set(ActivityConfiguration.IDENTIFIER, activityId)
                .set(ActivityConfiguration.ACTIVITY, SleepActivity.class)
                .build();
            eval.submitContextAndActivity(contextConfigBuilder.build(), activityConfig);
          } catch (final BindException ex) {
              LOG.log(Level.SEVERE, "Failed to submit Context to Evaluator: " + eval.getId(), ex);
              throw new RuntimeException(ex);
          }
        } else {
          LOG.log(Level.INFO, "TIME: Close Evaluator {0}", eval.getId());
          eval.close();
        }
      }
    }
  }

  /**
   * Receive notification that the Activity is running.
   */
  final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public void onNext(final RunningActivity act) {
      LOG.log(Level.INFO, "TIME: Running Activity {0}", act.getId());
    }
  }

  /**
   * Receive notification that the Activity has completed successfully.
   */
  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(final CompletedActivity act) {
      final ActiveContext context = act.getActiveContext();
      LOG.log(Level.INFO, "TIME: Completed Activity {0} on Evaluator {1}",
              new Object[] { act.getId(), context.getEvaluatorId() });
      synchronized (JobDriver.this) {
        if (numActivitiesStarted < numActivities) {
          ++numActivitiesStarted;
          final String activityId = String.format("Activity_%08d", numActivitiesStarted);
          LOG.log(Level.INFO, "TIME: Submit Activity {0} to Evaluator {1}",
                  new Object[] { activityId, context.getEvaluatorId() });
          try {
            final Configuration activityConfig = ActivityConfiguration.CONF
                .set(ActivityConfiguration.IDENTIFIER, activityId)
                .set(ActivityConfiguration.ACTIVITY, SleepActivity.class)
                .build();
            context.submitActivity(activityConfig);
          } catch (final BindException ex) {
            LOG.log(Level.SEVERE, "Failed to submit Activity to Context: " + context.getId(), ex);
            throw new RuntimeException(ex);
          }
        } else {
          LOG.log(Level.INFO, "TIME: Close Evaluator {0}", context.getEvaluatorId());
          context.close();
        }
      }
    }
  }

  /**
   * Receive notification that the Activity is running.
   */
  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      LOG.log(Level.INFO, "TIME: Completed Evaluator {0}", eval.getId());
    }
  }
}
