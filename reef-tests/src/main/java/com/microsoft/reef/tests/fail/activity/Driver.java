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
package com.microsoft.reef.tests.fail.activity;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.events.ActivityStart;
import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.activity.events.SuspendEvent;
import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.RunningActivity;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.tests.exceptions.DriverSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class Driver {

  /**
   * Name of the message class to specify the failing message handler.
   */
  @NamedParameter(doc = "Full name of the (failing) activity class", short_name = "activity")
  public static final class FailActivityName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());

  private static final Map<String, Class<? extends Activity>> FAIL_ACTIVITIES =
      new HashMap<String, Class<? extends Activity>>() {{
        for (final Class<? extends Activity> act : Arrays.asList(
            FailActivity.class,
            FailActivityStart.class,
            FailActivityCall.class,
            FailActivityMsg.class,
            FailActivitySuspend.class,
            FailActivityStop.class)) {
          put(act.getName(), act);
        }
      }};

  private final transient Class<? extends Activity> failActivity;
  private final transient EvaluatorRequestor requestor;
  private transient String activityId;

  @Inject
  public Driver(final @Parameter(FailActivityName.class) String failActivityName,
                final EvaluatorRequestor requestor) {
    this.failActivity = FAIL_ACTIVITIES.get(failActivityName);
    assert (this.failActivity != null);
    this.requestor = requestor;
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {

      try {

        activityId = failActivity.getSimpleName() + "_" + eval.getId();
        LOG.log(Level.INFO, "Submit activity: {0}", activityId);

        final Configuration contextConfig = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, activityId)
            .build();

        ConfigurationModule activityConfig = ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, activityId)
            .set(ActivityConfiguration.ACTIVITY, failActivity);

        switch (failActivity.getSimpleName()) {
          case "FailActivityMsg":
            LOG.log(Level.INFO, "MessageHandler: {0}", failActivity);
            activityConfig = activityConfig.set(
                ActivityConfiguration.ON_MESSAGE,
                (Class<? extends EventHandler<DriverMessage>>) failActivity);
            break;
          case "FailActivitySuspend":
            LOG.log(Level.INFO, "SuspendHandler: {0}", failActivity);
            activityConfig = activityConfig.set(
                ActivityConfiguration.ON_SUSPEND,
                (Class<? extends EventHandler<SuspendEvent>>) failActivity);
            break;
          case "FailActivityStart":
            LOG.log(Level.INFO, "StartHandler: {0}", failActivity);
            activityConfig = activityConfig.set(
                ActivityConfiguration.ON_ACTIVITY_STARTED,
                (Class<? extends EventHandler<ActivityStart>>) failActivity);
            break;
          case "FailActivityStop":
            LOG.log(Level.INFO, "StopHandler: {0}", failActivity);
            activityConfig = activityConfig.set(
                ActivityConfiguration.ON_ACTIVITY_STOP,
                (Class<? extends EventHandler<ActivityStop>>) failActivity);
            break;
        }

        eval.submitContextAndActivity(contextConfig, activityConfig.build());

      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Configuration error", ex);
        throw new DriverSideFailure("Configuration error", ex);
      }
    }
  }

  final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public void onNext(final RunningActivity act) {

      LOG.log(Level.INFO, "ActivityRuntime: {0} expect {1}",
          new Object[]{act.getId(), activityId});

      if (!activityId.equals(act.getId())) {
        throw new DriverSideFailure("Activity ID " + act.getId()
            + " not equal expected ID " + activityId);
      }

      switch (failActivity.getSimpleName()) {
        case "FailActivityMsg":
          LOG.log(Level.INFO, "ActivityRuntime: Send message: {0}", act);
          act.onNext(new byte[0]);
          break;
        case "FailActivitySuspend":
          LOG.log(Level.INFO, "ActivityRuntime: Suspend: {0}", act);
          act.suspend();
          break;
        case "FailActivityStop":
          LOG.log(Level.INFO, "ActivityRuntime: Stop: {0}", act);
          act.close();
          break;
      }
    }
  }
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) throws DriverSideFailure {
      throw new DriverSideFailure("Unexpected ActiveContext message: " + context.getId());
    }
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      Driver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
    }
  }
}
