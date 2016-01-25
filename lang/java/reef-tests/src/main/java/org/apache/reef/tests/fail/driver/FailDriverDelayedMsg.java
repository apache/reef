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

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver which fails due to delayed message.
 */
@Unit
public final class FailDriverDelayedMsg {

  private static final Logger LOG = Logger.getLogger(FailDriverDelayedMsg.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");

  private final transient EvaluatorRequestor requestor;
  private final transient Clock clock;
  private transient RunningTask task = null;

  @Inject
  public FailDriverDelayedMsg(final EvaluatorRequestor requestor, final Clock clock) {
    LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.<init>");
    this.requestor = requestor;
    this.clock = clock;
  }

  /**
   * Handler for AllocatedEvaluator.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eval) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(AllocatedEvaluator): {0}", eval);
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

  /**
   * Handler for ActiveContext.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(ActiveContext): {0}", context);
      try {
        context.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "Task_" + context.getId())
            .set(TaskConfiguration.TASK, NoopTask.class)
            .set(TaskConfiguration.ON_MESSAGE, NoopTask.DriverMessageHandler.class)
            .set(TaskConfiguration.ON_SUSPEND, NoopTask.TaskSuspendHandler.class)
            .set(TaskConfiguration.ON_TASK_STOP, NoopTask.TaskStopHandler.class)
            .set(TaskConfiguration.ON_CLOSE, NoopTask.TaskCloseHandler.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, NoopTask.class)
            .build());
      } catch (final BindException ex) {
        LOG.log(Level.WARNING, "Task configuration error", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Handler for RunningTask.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    @SuppressWarnings("checkstyle:hiddenfield")
    public void onNext(final RunningTask task) {
      FailDriverDelayedMsg.this.task = task;
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(TaskRuntime): {0}", task);
      FailDriverDelayedMsg.this.clock.scheduleAlarm(2000, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm time) {
          LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(Alarm): {0}", time);
          task.send(HELLO_STR);
        }
      });
    }
  }

  /**
   * Handler for TaskMessage.
   */
  public final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage msg) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(TaskMessage): {0}", msg);
      assert Arrays.equals(HELLO_STR, msg.get());
      FailDriverDelayedMsg.this.task.close();
    }
  }

  /**
   * Handler for StartTime.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.INFO, "ENTER: FailDriverDelayedMsg.send(StartTime): {0}", time);
      FailDriverDelayedMsg.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setMemory(128).setNumberOfCores(1).build());
    }
  }
}
