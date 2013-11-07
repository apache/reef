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
package com.microsoft.reef.tests.activityresubmit;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.FailedActivity;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.driver.contexts.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.tests.TestUtils;
import com.microsoft.reef.tests.exceptions.ActivitySideFailure;
import com.microsoft.reef.tests.exceptions.SimulatedActivityFailure;
import com.microsoft.reef.tests.fail.activity.FailActivityCall;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private int failuresSeen = 0;

  @Inject
  public Driver(EvaluatorRequestor evaluatorRequestor) {
    this.evaluatorRequestor = evaluatorRequestor;
  }

  private static Configuration getActivityConfiguration() {
    try {
      return ActivityConfiguration.CONF
          .set(ActivityConfiguration.ACTIVITY, FailActivityCall.class)
          .set(ActivityConfiguration.IDENTIFIER, "FailActivity")
          .build();
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
  }

  final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      Driver.this.evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setSize(EvaluatorRequest.Size.MEDIUM)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "ActivityResubmitContext")
            .build();
        final Configuration activityConfiguration = getActivityConfiguration();
        allocatedEvaluator.submitContextAndActivity(contextConfiguration, activityConfiguration);
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Activity configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  final class ActivityFailedHandler implements EventHandler<FailedActivity> {

    @Override
    public void onNext(final FailedActivity failedActivity) {

      LOG.log(Level.INFO, "FailedActivity: {0}", failedActivity);

      final Throwable ex = failedActivity.getReason().orElse(null);
      if (!TestUtils.hasCause(ex, SimulatedActivityFailure.class)) {
        final String msg = "Expected SimulatedActivityFailure from " + failedActivity.getId();
        LOG.log(Level.SEVERE, msg, ex);
        throw new ActivitySideFailure(msg, ex);
      }

      final ActiveContext activeContext = failedActivity.getActiveContext().get();
      if (++Driver.this.failuresSeen <= 1) { // resubmit the activity
        activeContext.submitActivity(getActivityConfiguration());
      } else { // Close the context
        activeContext.close();
      }
    }
  }
}
