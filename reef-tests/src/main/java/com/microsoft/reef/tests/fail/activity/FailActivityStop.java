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
import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.reef.tests.exceptions.ActivitySideFailure;
import com.microsoft.reef.tests.exceptions.SimulatedActivityFailure;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic activity that just fails when we stop it.
 */
public final class FailActivityStop implements Activity, EventHandler<ActivityStop> {

  private static final Logger LOG = Logger.getLogger(FailActivityStop.class.getName());

  private transient boolean isRunning = true;

  @Inject
  public FailActivityStop() {
    LOG.info("FailActivityStop created.");
  }

  @Override
  public byte[] call(final byte[] memento) {
    synchronized (this) {
      LOG.info("FailActivityStop.call() invoked. Waiting for the message.");
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
  public void onNext(final ActivityStop event) throws SimulatedActivityFailure {
    // synchronized (this) {
    //   this.isRunning = false;
    //   this.notify();
    // }
    final SimulatedActivityFailure ex = new SimulatedActivityFailure("FailActivityStop.onNext() invoked.");
    LOG.log(Level.WARNING, "FailActivityStop.onNext() invoked.", ex);
    throw ex;
  }
}
