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
package com.microsoft.reef.tests.messaging.activity;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.ActivityMessage;
import com.microsoft.reef.driver.activity.RunningActivity;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.tests.exceptions.DriverSideFailure;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class Driver {

  private static final Logger LOG = Logger.getLogger(Driver.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");
  private static final int DELAY = 1000; // send message to Activity 1 sec. after ActivityRuntime

  private final transient JobMessageObserver client;
  private final transient EvaluatorRequestor requestor;
  private final transient Clock clock;

  @Inject
  public Driver(final JobMessageObserver client, final EvaluatorRequestor requestor, final Clock clock) {
    this.client = client;
    this.requestor = requestor;
    this.clock = clock;
  }

  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator eval) {

      try {

        final String activityId = "Activity_" + eval.getId();
        LOG.log(Level.INFO, "Submit activity: {0}", activityId);

        final Configuration contextConfig = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, activityId)
            .build();

        final Configuration activityConfig = ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, activityId)
            .set(ActivityConfiguration.ACTIVITY, ActivityMsg.class)
            .set(ActivityConfiguration.ON_MESSAGE, ActivityMsg.DriverMessageHandler.class)
            .set(ActivityConfiguration.ON_GET_MESSAGE, ActivityMsg.class)
            .build();

        eval.submitContextAndActivity(contextConfig, activityConfig);

      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  public final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public void onNext(final RunningActivity act) {
      LOG.log(Level.INFO, "ActivityRuntime: {0}", act.getId());
      clock.scheduleAlarm(DELAY, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm alarm) {
          act.onNext(HELLO_STR);
        }
      });
    }
  }

  public final class ActivityMessageHandler implements EventHandler<ActivityMessage> {
    @Override
    public void onNext(final ActivityMessage msg) {
      LOG.log(Level.INFO, "ActivityMessage: from {0}: {1}",
          new Object[]{msg.getId(), CODEC.decode(msg.get())});
      if (!Arrays.equals(msg.get(), HELLO_STR)) {
        final Exception ex = new DriverSideFailure("Unexpected message: " + CODEC.decode(msg.get()));
        LOG.log(Level.SEVERE, "Bad message from " + msg.getId(), ex);
        client.onError(ex);
      }
    }
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "StartTime: {0}", time);
      Driver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
    }
  }
}
