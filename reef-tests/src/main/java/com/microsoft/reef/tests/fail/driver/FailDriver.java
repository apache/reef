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

import com.microsoft.reef.driver.activity.*;
import com.microsoft.reef.driver.context.*;
import com.microsoft.reef.driver.evaluator.*;
import com.microsoft.reef.tests.exceptions.DriverSideFailure;
import com.microsoft.reef.tests.exceptions.SimulatedDriverFailure;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
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
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.microsoft.reef.tests.fail.driver.FailDriver.ExpectedMessage.RequiredFlag.OPTIONAL;
import static com.microsoft.reef.tests.fail.driver.FailDriver.ExpectedMessage.RequiredFlag.REQUIRED;

@Unit
public final class FailDriver {

  /**
   * Name of the message class to specify the failing message handler.
   */
  @NamedParameter(doc = "Full name of the message class to fail on", short_name = "fail")
  public static final class FailMsgClassName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(FailDriver.class.getName());

  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");


  /**
   * Send message to the Activity MSG_DELAY milliseconds after start.
   */
  private static final int MSG_DELAY = 1000;

  public static final class ExpectedMessage {

    public enum RequiredFlag {OPTIONAL, REQUIRED}

    public final transient Class<?> msgClass;
    public final transient RequiredFlag requiredFlag;
    private final transient String repr;

    public ExpectedMessage(final Class<?> clazz, final RequiredFlag requiredFlag) {
      this.msgClass = clazz;
      this.requiredFlag = requiredFlag;
      this.repr = this.msgClass.getSimpleName() + ":" + this.requiredFlag;
    }

    @Override
    public String toString() {
      return this.repr;
    }
  }

  private static final ExpectedMessage[] EVENT_SEQUENCE = {
      new ExpectedMessage(FailDriver.class, REQUIRED),
      new ExpectedMessage(StartTime.class, REQUIRED),
      new ExpectedMessage(AllocatedEvaluator.class, REQUIRED),
      new ExpectedMessage(FailedEvaluator.class, OPTIONAL),
      new ExpectedMessage(ActiveContext.class, REQUIRED),
      new ExpectedMessage(ContextMessage.class, OPTIONAL),
      new ExpectedMessage(FailedContext.class, OPTIONAL),
      new ExpectedMessage(RunningActivity.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(ActivityMessage.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(SuspendedActivity.class, REQUIRED),
      new ExpectedMessage(RunningActivity.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(FailedActivity.class, OPTIONAL),
      new ExpectedMessage(CompletedActivity.class, REQUIRED),
      new ExpectedMessage(ClosedContext.class, OPTIONAL),
      new ExpectedMessage(CompletedEvaluator.class, REQUIRED),
      new ExpectedMessage(StopTime.class, REQUIRED)
  };

  private final transient Class<?> failMsgClass;
  private final transient EvaluatorRequestor requestor;
  private final transient Clock clock;

  private transient RunningActivity activity = null;
  private transient int expectIdx = 0;

  private enum DriverState {INIT, SEND_MSG, SUSPEND, RESUME, CLOSE, FAILED}

  private transient DriverState state = DriverState.INIT;

  @Inject
  public FailDriver(final @Parameter(FailMsgClassName.class) String failMsgClassName,
                    final EvaluatorRequestor requestor, final Clock clock)
      throws ClassNotFoundException {
    this.failMsgClass = ClassLoader.getSystemClassLoader().loadClass(failMsgClassName);
    this.requestor = requestor;
    this.clock = clock;
    this.checkMsgOrder(this);
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      checkMsgOrder(eval);
      try {
        eval.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "FailContext_" + eval.getId())
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Context configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      checkMsgOrder(eval);
      // noop
    }
  }

  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator eval) {
      LOG.log(Level.WARNING, "Evaluator failed: " + eval.getId(), eval.getEvaluatorException());
      checkMsgOrder(eval);
      throw new RuntimeException(eval.getEvaluatorException());
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      checkMsgOrder(context);
      try {
        context.submitActivity(ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, "FailActivity_" + context.getId())
            .set(ActivityConfiguration.ACTIVITY, NoopActivity.class)
            .set(ActivityConfiguration.ON_MESSAGE, NoopActivity.DriverMessageHandler.class)
            .set(ActivityConfiguration.ON_SUSPEND, NoopActivity.ActivitySuspendHandler.class)
            .set(ActivityConfiguration.ON_CLOSE, NoopActivity.ActivityCloseHandler.class)
            .set(ActivityConfiguration.ON_ACTIVITY_STOP, NoopActivity.ActivityStopHandler.class)
            .set(ActivityConfiguration.ON_GET_MESSAGE, NoopActivity.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Activity configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage message) {
      checkMsgOrder(message);
      // noop
    }
  }

  final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext context) {
      checkMsgOrder(context);
      // noop
    }
  }

  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext context) {
      LOG.log(Level.WARNING, "Context failed: " + context.getId(), context.getReason());
      checkMsgOrder(context);
      // TODO: notify client?

      // if (context.getParentContext().isPresent()) {
      //   context.getParentContext().get().close();
      // }
    }
  }

  final class RunningActivityHandler implements EventHandler<RunningActivity> {
    @Override
    public void onNext(final RunningActivity act) {
      checkMsgOrder(act);
      activity = act;
      switch (state) {
        case INIT:
          state = DriverState.SEND_MSG;
          break;
        case RESUME:
          state = DriverState.CLOSE;
          break;
        default:
          LOG.log(Level.WARNING, "Unexpected state at ActivityRuntime: {0}", state);
          throw new DriverSideFailure("Unexpected state: " + state);
      }
      // After a delay, send message or suspend the activity:
      clock.scheduleAlarm(MSG_DELAY, new AlarmHandler());
    }
  }

  final class SuspendedActivityHandler implements EventHandler<SuspendedActivity> {
    @Override
    public void onNext(final SuspendedActivity act) {
      checkMsgOrder(act);
      state = DriverState.RESUME;
      try {
        act.getActiveContext().submitActivity(ActivityConfiguration.CONF
            .set(ActivityConfiguration.IDENTIFIER, act.getId() + "_RESUMED")
            .set(ActivityConfiguration.ACTIVITY, NoopActivity.class)
            .set(ActivityConfiguration.ON_MESSAGE, NoopActivity.DriverMessageHandler.class)
            .set(ActivityConfiguration.ON_SUSPEND, NoopActivity.ActivitySuspendHandler.class)
            .set(ActivityConfiguration.ON_CLOSE, NoopActivity.ActivityCloseHandler.class)
            .set(ActivityConfiguration.ON_ACTIVITY_STOP, NoopActivity.ActivityStopHandler.class)
            .set(ActivityConfiguration.ON_GET_MESSAGE, NoopActivity.class)
            .set(ActivityConfiguration.MEMENTO, DatatypeConverter.printBase64Binary(HELLO_STR))
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Activity configuration error", ex);
        throw new DriverSideFailure("Activity configuration error", ex);
      }
    }
  }

  final class ActivityMessageHandler implements EventHandler<ActivityMessage> {
    @Override
    public void onNext(final ActivityMessage msg) {
      checkMsgOrder(msg);
      assert (Arrays.equals(HELLO_STR, msg.get()));
      assert (state == DriverState.SEND_MSG);
      state = DriverState.SUSPEND;
      clock.scheduleAlarm(MSG_DELAY, new AlarmHandler());
    }
  }

  final class FailedActivityHandler implements EventHandler<FailedActivity> {
    @Override
    public void onNext(final FailedActivity act) {
      LOG.log(Level.WARNING, "Activity failed: " + act.getId(), act.getReason().orElse(null));
      checkMsgOrder(act);
      if (act.getActiveContext().isPresent()) {
        act.getActiveContext().get().close();
      }
    }
  }

  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(final CompletedActivity act) {
      checkMsgOrder(act);
      act.getActiveContext().close();
    }
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      FailDriver.this.checkMsgOrder(time);
      FailDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
    }
  }

  final class AlarmHandler implements EventHandler<Alarm> {
    @Override
    public void onNext(final Alarm time) {
      FailDriver.this.checkMsgOrder(time);
      switch (FailDriver.this.state) {
        case SEND_MSG:
          FailDriver.this.activity.onNext(HELLO_STR);
          break;
        case SUSPEND:
          FailDriver.this.activity.suspend();
          break;
        case CLOSE:
          FailDriver.this.activity.close();
          break;
        default:
          LOG.log(Level.WARNING, "Unexpected state at AlarmHandler: {0}", FailDriver.this.state);
          throw new DriverSideFailure("Unexpected state: " + FailDriver.this.state);
      }
    }
  }

  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime time) {
      FailDriver.this.checkMsgOrder(time);
      // noop
    }
  }

  /**
   * Check if observer methods are called in the right order
   * and generate an exception at the given point in the message sequence.
   *
   * @param msg a message from one of the observers.
   * @throws SimulatedDriverFailure if failMsgClass matches the message class.
   * @throws DriverSideFailure      if messages are out of order.
   */
  private void checkMsgOrder(final Object msg) throws SimulatedDriverFailure, DriverSideFailure {

    final String msgClassName = msg.getClass().getName();
    LOG.log(Level.FINE, "At {0} {1}:{2}", new Object[]{
        this.state, this.expectIdx, msgClassName});

    if (this.state == DriverState.FAILED) {
      // If already failed, do not do anything
      return;
    }

    // Simulate failure at this step?
    if (this.failMsgClass.isInstance(msg)) {
      this.state = DriverState.FAILED;
      if (msg instanceof StartTime) {
        try {
          Thread.sleep(200); // TODO #389 #420
        } catch (final InterruptedException e) {

        }
      }
    }

    // Make sure events arrive in the right order (specified in EVENT_SEQUENCE):
    boolean notFound = true;
    for (; this.expectIdx < EVENT_SEQUENCE.length; ++this.expectIdx) {
      if (EVENT_SEQUENCE[expectIdx].msgClass.isInstance(msg)) {
        notFound = false;
        break;
      } else if (EVENT_SEQUENCE[expectIdx].requiredFlag == REQUIRED) {
        break;
      }
    }

    if (notFound) {
      LOG.log(Level.SEVERE, "Event out of sequence: {0} {1}:{2}",
          new Object[]{this.state, this.expectIdx, msgClassName});
      throw new DriverSideFailure("Event out of sequence: " + msgClassName);
    }

    LOG.log(Level.INFO, "{0}: onNext: {1} got: {2}", new Object[]{
        this.state, EVENT_SEQUENCE[this.expectIdx], msgClassName});

    ++this.expectIdx;

    if (this.state == DriverState.FAILED) {
      final SimulatedDriverFailure ex = new SimulatedDriverFailure(
          "Simulated Failure at FailDriver :: " + msgClassName);
      LOG.log(Level.SEVERE, "Simulated Failure", ex);
      throw ex;
    }
  }
}
