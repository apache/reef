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
package org.apache.reef.examples.suspend;

import org.apache.reef.io.checkpoint.CheckpointID;
import org.apache.reef.io.checkpoint.CheckpointService;
import org.apache.reef.io.checkpoint.CheckpointService.CheckpointReadChannel;
import org.apache.reef.io.checkpoint.CheckpointService.CheckpointWriteChannel;
import org.apache.reef.io.checkpoint.fs.FSCheckpointID;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.SuspendEvent;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple do-nothing task that can send messages to the Driver and can be suspended/resumed.
 */
@Unit
public class SuspendTestTask implements Task, TaskMessageSource {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(SuspendTestTask.class.getName());
  private final CheckpointService checkpointService;
  /**
   * number of cycles to run in the task.
   */
  private final int numCycles;
  /**
   * delay in milliseconds between cycles in the task.
   */
  private final int delay;
  /**
   * Codec to serialize/deserialize counter values for the updates.
   */
  private final ObjectSerializableCodec<Integer> codecInt = new ObjectSerializableCodec<>();
  /**
   * Codec to serialize/deserialize checkpoint IDs for suspend/resume.
   */
  @SuppressWarnings("checkstyle:diamondoperatorforvariabledefinition")
  private final ObjectWritableCodec<CheckpointID> codecCheckpoint =
      new ObjectWritableCodec<CheckpointID>(FSCheckpointID.class);
  /**
   * Current value of the counter.
   */
  private int counter = 0;
  /**
   * True if the suspend message has been received, false otherwise.
   */
  private boolean suspended = false;

  /**
   * Task constructor: invoked by TANG.
   *
   * @param numCycles number of cycles to run in the task.
   * @param delay     delay in seconds between cycles in the task.
   */
  @Inject
  public SuspendTestTask(
      final CheckpointService checkpointService,
      @Parameter(Launch.NumCycles.class) final int numCycles,
      @Parameter(Launch.Delay.class) final int delay) {
    this.checkpointService = checkpointService;
    this.numCycles = numCycles;
    this.delay = delay * 1000;
  }

  /**
   * Main method of the task: run cycle from 0 to numCycles,
   * and sleep for delay seconds on each cycle.
   *
   * @param memento serialized version of the counter.
   *                Empty array for initial run, but can contain value for resumed job.
   * @return serialized version of the counter.
   */
  @Override
  public synchronized byte[] call(final byte[] memento) throws IOException, InterruptedException {

    LOG.log(Level.INFO, "Start: {0} counter: {1}/{2}",
        new Object[]{this, this.counter, this.numCycles});

    if (memento != null && memento.length > 0) {
      this.restore(memento);
    }

    this.suspended = false;
    for (; this.counter < this.numCycles && !this.suspended; ++this.counter) {
      try {
        LOG.log(Level.INFO, "Run: {0} counter: {1}/{2} sleep: {3}",
            new Object[]{this, this.counter, this.numCycles, this.delay});
        this.wait(this.delay);
      } catch (final InterruptedException ex) {
        LOG.log(Level.INFO, "{0} interrupted. counter: {1}: {2}",
            new Object[]{this, this.counter, ex});
      }
    }

    return this.suspended ? this.save() : this.codecInt.encode(this.counter);
  }

  /**
   * Update driver on current state of the task.
   *
   * @return serialized version of the counter.
   */
  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    LOG.log(Level.INFO, "Message from Task {0} to the Driver: counter: {1}",
        new Object[]{this, this.counter});
    return Optional.of(TaskMessage.from(SuspendTestTask.class.getName(), this.codecInt.encode(this.counter)));
  }

  /**
   * Save current state of the task in the checkpoint.
   *
   * @return checkpoint ID (serialized)
   */
  private synchronized byte[] save() throws IOException, InterruptedException {
    try (final CheckpointWriteChannel channel = this.checkpointService.create()) {
      channel.write(ByteBuffer.wrap(this.codecInt.encode(this.counter)));
      return this.codecCheckpoint.encode(this.checkpointService.commit(channel));
    }
  }

  /**
   * Restore the task state from the given checkpoint.
   *
   * @param memento serialized checkpoint ID
   */
  private synchronized void restore(final byte[] memento) throws IOException, InterruptedException {
    final CheckpointID checkpointId = this.codecCheckpoint.decode(memento);
    try (final CheckpointReadChannel channel = this.checkpointService.open(checkpointId)) {
      final ByteBuffer buffer = ByteBuffer.wrap(this.codecInt.encode(this.counter));
      channel.read(buffer);
      this.counter = this.codecInt.decode(buffer.array());
    }
    this.checkpointService.delete(checkpointId);
  }

  /**
   * Handler for suspend event.
   */
  public class SuspendHandler implements EventHandler<SuspendEvent> {
    @Override
    public void onNext(final SuspendEvent suspendEvent) {
      synchronized (SuspendTestTask.this) {
        LOG.log(Level.INFO, "Suspend: {0}; counter: {1}",
            new Object[]{this, SuspendTestTask.this.counter});
        SuspendTestTask.this.suspended = true;
        SuspendTestTask.this.notify();
      }
    }
  }

}
