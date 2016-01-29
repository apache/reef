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

import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.task.events.SuspendEvent;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic task that quite successfully does nothing.
 */
@Unit
public final class NoopTask implements Task, TaskMessageSource {

  private static final Logger LOG = Logger.getLogger(NoopTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient boolean isRunning = true;
  private transient Optional<TaskMessage> message = Optional.empty();

  @Inject
  public NoopTask() {
    LOG.info("NoopTask created.");
  }

  @Override
  public synchronized byte[] call(final byte[] memento) {
    this.isRunning = true;
    while (this.isRunning) {
      try {
        LOG.info("NoopTask.call(): Waiting for the message.");
        this.wait();
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "NoopTask.wait() interrupted.", ex);
      }
    }
    LOG.log(Level.INFO, "NoopTask.call(): Exiting with message {0}",
        CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    return this.message.orElse(INIT_MESSAGE).get();
  }

  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    LOG.log(Level.INFO, "NoopTask.getMessage() invoked: {0}",
        CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    return this.message;
  }

  private synchronized void stopTask() {
    LOG.info("NoopTask.stopTask() invoked.");
    this.isRunning = false;
    this.notify();
  }

  /**
   * Handler for SuspendEvent.
   */
  public class TaskSuspendHandler implements EventHandler<SuspendEvent> {
    @Override
    public void onNext(final SuspendEvent suspendEvent) {
      LOG.info("NoopTask.TaskSuspendHandler.send() invoked.");
      NoopTask.this.stopTask();
    }
  }

  /**
   * Handler for TaskStop.
   */
  public class TaskStopHandler implements EventHandler<TaskStop> {
    @Override
    public void onNext(final TaskStop event) {
      LOG.info("NoopTask.TaskStopHandler.send() invoked.");
      NoopTask.this.stopTask();
    }
  }

  /**
   * Handler for CloseEvent.
   */
  public class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOG.info("NoopTask.TaskCloseHandler.send() invoked.");
      NoopTask.this.stopTask();
    }
  }

  /**
   * Handler for DriverMessage.
   */
  public class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage driverMessage) {
      final byte[] msg = driverMessage.get().get();
      LOG.log(Level.INFO, "NoopTask.DriverMessageHandler.send() invoked: {0}", CODEC.decode(msg));
      synchronized (NoopTask.this) {
        NoopTask.this.message = Optional.of(TaskMessage.from(NoopTask.this.toString(), msg));
      }
    }
  }
}
