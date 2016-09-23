/*
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
package org.apache.reef.tests.fail.driver;

import org.apache.reef.driver.context.*;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.tests.library.exceptions.SimulatedDriverFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.reef.tests.fail.driver.FailDriver.ExpectedMessage.RequiredFlag.OPTIONAL;
import static org.apache.reef.tests.fail.driver.FailDriver.ExpectedMessage.RequiredFlag.REQUIRED;

/**
 * Driver which fails on receiving certain message class.
 */
@Unit
public final class FailDriver {

  private static final Logger LOG = Logger.getLogger(FailDriver.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");
  /**
   * Send message to the Task MSG_DELAY milliseconds after start.
   */
  private static final int MSG_DELAY = 1000;
  private static final ExpectedMessage[] EVENT_SEQUENCE = {
      new ExpectedMessage(FailDriver.class, REQUIRED),
      new ExpectedMessage(StartTime.class, REQUIRED),
      new ExpectedMessage(AllocatedEvaluator.class, REQUIRED),
      new ExpectedMessage(FailedEvaluator.class, OPTIONAL),
      new ExpectedMessage(ActiveContext.class, REQUIRED),
      new ExpectedMessage(ContextMessage.class, OPTIONAL),
      new ExpectedMessage(FailedContext.class, OPTIONAL),
      new ExpectedMessage(RunningTask.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(TaskMessage.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(SuspendedTask.class, REQUIRED),
      new ExpectedMessage(RunningTask.class, REQUIRED),
      new ExpectedMessage(Alarm.class, REQUIRED),
      new ExpectedMessage(FailedTask.class, OPTIONAL),
      new ExpectedMessage(CompletedTask.class, REQUIRED),
      new ExpectedMessage(ClosedContext.class, OPTIONAL),
      new ExpectedMessage(CompletedEvaluator.class, REQUIRED),
      new ExpectedMessage(StopTime.class, REQUIRED)
  };
  private final transient Class<?> failMsgClass;
  private final transient EvaluatorRequestor requestor;
  private final transient Clock clock;
  private transient RunningTask task = null;
  private transient int expectIdx = 0;
  private transient DriverState state = DriverState.INIT;

  @Inject
  public FailDriver(@Parameter(FailMsgClassName.class) final String failMsgClassName,
                    final EvaluatorRequestor requestor, final Clock clock)
      throws ClassNotFoundException {
    this.failMsgClass = ClassLoader.getSystemClassLoader().loadClass(failMsgClassName);
    this.requestor = requestor;
    this.clock = clock;
    this.checkMsgOrder(this);
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

    LOG.log(Level.INFO, "{0}: send: {1} got: {2}", new Object[]{
        this.state, EVENT_SEQUENCE[this.expectIdx], msgClassName});

    ++this.expectIdx;

    if (this.state == DriverState.FAILED) {
      final SimulatedDriverFailure ex = new SimulatedDriverFailure(
          "Simulated Failure at FailDriver :: " + msgClassName);
      LOG.log(Level.INFO, "Simulated Failure:", ex);
      throw ex;
    }
  }

  private enum DriverState {INIT, SEND_MSG, SUSPEND, RESUME, CLOSE, FAILED}

  /**
   * Name of the message class to specify the failing message handler.
   */
  @NamedParameter(doc = "Full name of the message class to fail on", short_name = "fail")
  public static final class FailMsgClassName implements Name<String> {
  }

  /**
   * Expected message class.
   */
  public static final class ExpectedMessage {

    private final transient Class<?> msgClass;
    private final transient RequiredFlag requiredFlag;
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

    /**
     * "Required" flag for message class.
     */
    public enum RequiredFlag {OPTIONAL, REQUIRED}
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
        context.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "FailTask_" + context.getId())
            .set(TaskConfiguration.TASK, NoopTask.class)
            .set(TaskConfiguration.ON_MESSAGE, NoopTask.DriverMessageHandler.class)
            .set(TaskConfiguration.ON_SUSPEND, NoopTask.TaskSuspendHandler.class)
            .set(TaskConfiguration.ON_CLOSE, NoopTask.TaskCloseHandler.class)
            .set(TaskConfiguration.ON_TASK_STOP, NoopTask.TaskStopHandler.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, NoopTask.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Task configuration error", ex);
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
      LOG.log(Level.WARNING, "Context failed: " + context.getId(), context.getReason().orElse(null));
      checkMsgOrder(context);

      // if (context.getParentContext().isPresent()) {
      //   context.getParentContext().get().close();
      // }
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    @SuppressWarnings("checkstyle:hiddenfield")
    public void onNext(final RunningTask task) {
      checkMsgOrder(task);
      FailDriver.this.task = task;
      switch (state) {
      case INIT:
        state = DriverState.SEND_MSG;
        break;
      case RESUME:
        state = DriverState.CLOSE;
        break;
      default:
        LOG.log(Level.WARNING, "Unexpected state at TaskRuntime: {0}", state);
        throw new DriverSideFailure("Unexpected state: " + state);
      }
      // After a delay, send message or suspend the task:
      clock.scheduleAlarm(MSG_DELAY, new AlarmHandler());
    }
  }

  final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    @SuppressWarnings("checkstyle:hiddenfield")
    public void onNext(final SuspendedTask task) {
      checkMsgOrder(task);
      state = DriverState.RESUME;
      try {
        task.getActiveContext().submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, task.getId() + "_RESUMED")
            .set(TaskConfiguration.TASK, NoopTask.class)
            .set(TaskConfiguration.ON_MESSAGE, NoopTask.DriverMessageHandler.class)
            .set(TaskConfiguration.ON_SUSPEND, NoopTask.TaskSuspendHandler.class)
            .set(TaskConfiguration.ON_CLOSE, NoopTask.TaskCloseHandler.class)
            .set(TaskConfiguration.ON_TASK_STOP, NoopTask.TaskStopHandler.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, NoopTask.class)
            .set(TaskConfiguration.MEMENTO, DatatypeConverter.printBase64Binary(HELLO_STR))
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.SEVERE, "Task configuration error", ex);
        throw new DriverSideFailure("Task configuration error", ex);
      }
    }
  }

  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage msg) {
      checkMsgOrder(msg);
      assert Arrays.equals(HELLO_STR, msg.get());
      assert state == DriverState.SEND_MSG;
      state = DriverState.SUSPEND;
      clock.scheduleAlarm(MSG_DELAY, new AlarmHandler());
    }
  }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    @SuppressWarnings("checkstyle:hiddenfield")
    public void onNext(final FailedTask task) {
      LOG.log(Level.WARNING, "Task failed: " + task.getId(), task.getReason().orElse(null));
      checkMsgOrder(task);
      if (task.getActiveContext().isPresent()) {
        task.getActiveContext().get().close();
      }
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    @SuppressWarnings("checkstyle:hiddenfield")
    public void onNext(final CompletedTask task) {
      checkMsgOrder(task);
      task.getActiveContext().close();
    }
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      FailDriver.this.checkMsgOrder(time);
      FailDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(128).setNumberOfCores(1).build());
    }
  }

  final class AlarmHandler implements EventHandler<Alarm> {
    @Override
    public void onNext(final Alarm time) {
      FailDriver.this.checkMsgOrder(time);
      switch (FailDriver.this.state) {
      case SEND_MSG:
        FailDriver.this.task.send(HELLO_STR);
        break;
      case SUSPEND:
        FailDriver.this.task.suspend();
        break;
      case CLOSE:
        FailDriver.this.task.close();
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
}
