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
package com.microsoft.reef.tests.fail.driver;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.ActivityMessage;
import com.microsoft.reef.driver.activity.RunningActivity;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
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
public final class FailDriverDelayedMsg {

  private static final Logger LOG = Logger.getLogger(FailDriverDelayedMsg.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");

  private final transient EvaluatorRequestor requestor;
  private final transient Clock clock;
  private transient RunningActivity activity = null;

  @Inject
  public FailDriverDelayedMsg(final EvaluatorRequestor requestor, final Clock clock) {
    LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.<init>");
    this.requestor = requestor;
    this.clock = clock;
  }

  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(AllocatedEvaluator): {0}", eval);
      try {
        eval.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "Context_" + eval.getId())
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Context configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(ActiveContext): {0}", context);
      try {
        context.submitActivity(ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, "Activity_" + context.getId())
            .set(ActivityConfiguration.ACTIVITY, NoopActivity.class)
            .set(ActivityConfiguration.ON_MESSAGE, NoopActivity.DriverMessageHandler.class)
            .set(ActivityConfiguration.ON_SUSPEND, NoopActivity.ActivitySuspendHandler.class)
            .set(ActivityConfiguration.ON_ACTIVITY_STOP, NoopActivity.ActivityStopHandler.class)
            .set(ActivityConfiguration.ON_CLOSE, NoopActivity.ActivityCloseHandler.class)
            .set(ActivityConfiguration.ON_SEND_MESSAGE, NoopActivity.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Activity configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  public final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public void onNext(final RunningActivity act) {
      FailDriverDelayedMsg.this.activity = act;
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(ActivityRuntime): {0}", act);
      FailDriverDelayedMsg.this.clock.scheduleAlarm(2000, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm time) {
          LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(Alarm): {0}", time);
          act.onNext(HELLO_STR);
        }
      });
    }
  }

  public final class ActivityMessageHandler implements EventHandler<ActivityMessage> {
    @Override
    public void onNext(final ActivityMessage msg) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(ActivityMessage): {0}", msg);
      assert (Arrays.equals(HELLO_STR, msg.get()));
      FailDriverDelayedMsg.this.activity.close();
    }
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.onNext(StartTime): {0}", time);
      FailDriverDelayedMsg.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
    }
  }
}
