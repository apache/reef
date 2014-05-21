/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.tests.fail.task;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.events.TaskStop;
import com.microsoft.reef.tests.exceptions.SimulatedTaskFailure;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic task that just fails when we stop it.
 */
public final class FailTaskStop implements Task, EventHandler<TaskStop> {

  private static final Logger LOG = Logger.getLogger(FailTaskStop.class.getName());

  private transient boolean isRunning = true;

  @Inject
  public FailTaskStop() {
    LOG.info("FailTaskStop created.");
  }

  @Override
  public byte[] call(final byte[] memento) {
    synchronized (this) {
      LOG.info("FailTaskStop.call() invoked. Waiting for the message.");
      while (this.isRunning) {
        try {
          this.wait();
        } catch (final InterruptedException ex) {
          LOG.log(Level.WARNING, "wait() interrupted.", ex);
        }
      }
    }
    return new byte[0];
  }

  @Override
  public void onNext(final TaskStop event) throws SimulatedTaskFailure {
    // synchronized (this) {
    //   this.isRunning = false;
    //   this.notify();
    // }
    final SimulatedTaskFailure ex = new SimulatedTaskFailure("FailTaskStop.send() invoked.");
    LOG.log(Level.FINE, "FailTaskStop.send() invoked: {0}", ex);
    throw ex;
  }
}
