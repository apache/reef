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
package org.apache.reef.tests.messaging.task;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver for TaskMessagingTest.
 */
@Unit
public final class TaskMessagingDriver {

  private static final Logger LOG = Logger.getLogger(TaskMessagingDriver.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final byte[] HELLO_STR = CODEC.encode("MESSAGE::HELLO");
  private static final int DELAY = 1000; // send message to Task 1 sec. after TaskRuntime

  private final transient JobMessageObserver client;
  private final transient Clock clock;

  @Inject
  public TaskMessagingDriver(final JobMessageObserver client, final Clock clock) {
    this.client = client;
    this.clock = clock;
  }

  /**
   * Handler for AllocatedEvaluator.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator eval) {
      final String taskId = "Task_" + eval.getId();
      LOG.log(Level.INFO, "Submit task: {0}", taskId);

      final Configuration taskConfig = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, TaskMessagingTask.class)
          .set(TaskConfiguration.ON_MESSAGE, TaskMessagingTask.DriverMessageHandler.class)
          .set(TaskConfiguration.ON_SEND_MESSAGE, TaskMessagingTask.class)
          .build();
      eval.submitTask(taskConfig);
    }
  }

  /**
   * Handler for RunningTask.
   */
  public final class TaskRunningHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOG.log(Level.FINE, "TaskRuntime: {0}", task.getId());
      clock.scheduleAlarm(DELAY, new EventHandler<Alarm>() {
        @Override
        public void onNext(final Alarm alarm) {
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
      LOG.log(Level.FINE, "TaskMessage: from {0}: {1}",
          new Object[]{msg.getId(), CODEC.decode(msg.get())});
      if (!Arrays.equals(msg.get(), HELLO_STR)) {
        final RuntimeException ex = new DriverSideFailure("Unexpected message: " + CODEC.decode(msg.get()));
        LOG.log(Level.SEVERE, "Bad message from " + msg.getId(), ex);
        throw ex;
      }
    }
  }
}
