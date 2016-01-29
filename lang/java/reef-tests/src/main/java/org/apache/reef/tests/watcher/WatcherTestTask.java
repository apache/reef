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
package org.apache.reef.tests.watcher;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.SuspendEvent;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.charset.Charset;

/**
 * Task for WatcherTest.
 */
@Unit
public final class WatcherTestTask implements Task, TaskMessageSource {

  private final TaskMessage taskMessage;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final boolean isTaskSuspended;
  private boolean isRunning;

  @Inject
  private WatcherTestTask(final HeartBeatTriggerManager heartBeatTriggerManager,
                          @Parameter(IsTaskSuspended.class) final boolean isTaskSuspended) {
    this.taskMessage = TaskMessage.from("MESSAGE_SOURCE", "MESSAGE".getBytes(Charset.forName("UTF-8")));
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.isTaskSuspended = isTaskSuspended;
    this.isRunning = true;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    if (isTaskSuspended) {
      synchronized (this) {
        while (isRunning) {
          wait();
        }
      }
    } else {
      heartBeatTriggerManager.triggerHeartBeat();
    }

    return null;
  }

  @Override
  public Optional<TaskMessage> getMessage() {
    return Optional.of(taskMessage);
  }

  /**
   * Handler for SuspendEvent.
   */
  public final class TaskSuspendedHandler implements EventHandler<SuspendEvent> {

    @Override
    public void onNext(final SuspendEvent value) {
      synchronized (WatcherTestTask.this) {
        isRunning = false;
        WatcherTestTask.this.notify();
      }
    }
  }
}
