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

package com.microsoft.reef.tests.fail.activity;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.tests.exceptions.SimulatedActivityFailure;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic activity that just fails when we send it a message.
 */
public final class FailActivityMsg implements Activity, EventHandler<DriverMessage> {

  private static final Logger LOG = Logger.getLogger(FailActivityMsg.class.getName());
  private transient boolean isRunning = true;

  @Inject
  public FailActivityMsg() {
    LOG.info("FailActivityMsg created.");
  }

  @Override
  public byte[] call(final byte[] memento) {
    synchronized (this) {
      LOG.info("FailActivityMsg.call() invoked. Waiting for the message.");
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
  public void onNext(final DriverMessage driverMessage) throws SimulatedActivityFailure {
    final SimulatedActivityFailure ex = new SimulatedActivityFailure("FailActivityMsg.onNext() invoked.");
    LOG.log(Level.WARNING, "FailActivityMsg.onNext() invoked.", ex);
    throw ex;
  }
}

