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

import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic task that receives a message and sends it back to the driver.
 */
@Unit
public final class TaskMessagingTask implements Task, TaskMessageSource {

  private static final Logger LOG = Logger.getLogger(TaskMessagingTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient boolean isRunning = true;
  private transient Optional<TaskMessage> message = Optional.empty();

  @Inject
  public TaskMessagingTask() {
    LOG.info("TaskMsg created.");
  }

  @Override
  public synchronized byte[] call(final byte[] memento) {
    LOG.info("TaskMsg.call() invoked. Waiting for the message.");
    while (this.isRunning) {
      try {
        this.wait();
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "wait() interrupted.", ex);
      }
    }
    return this.message.orElse(INIT_MESSAGE).get();
  }

  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    LOG.log(Level.INFO, "TaskMsg.getMessage() invoked: {0}",
        CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    if (this.message.isPresent()) {
      this.isRunning = false;
      this.notify();
    }
    return this.message;
  }

  /**
   * Handler for DriverMessage.
   */
  public class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage driverMessage) {
      final byte[] driverMsg = driverMessage.get().get();
      LOG.log(Level.INFO, "TaskMsg.send() invoked: {0}", CODEC.decode(driverMsg));
      TaskMessagingTask.this.message = Optional.of(TaskMessage.from(this.toString(), driverMsg));
    }
  }
}
