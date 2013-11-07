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

package com.microsoft.reef.tests.fail.driver;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.reef.activity.events.CloseEvent;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.activity.events.SuspendEvent;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic activity that quite successfully does nothing.
 */
@Unit
public final class NoopActivity implements Activity, ActivityMessageSource {

  private static final Logger LOG = Logger.getLogger(NoopActivity.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final ActivityMessage INIT_MESSAGE = ActivityMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient boolean isRunning = true;
  private transient Optional<ActivityMessage> message = Optional.empty();

  @Inject
  public NoopActivity() {
    LOG.info("NoopActivity created.");
  }

  @Override
  public synchronized byte[] call(final byte[] memento) {
    this.isRunning = true;
    while (this.isRunning) {
      try {
        LOG.info("NoopActivity.call(): Waiting for the message.");
        this.wait();
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "NoopActivity.wait() interrupted.", ex);
      }
    }
    LOG.log(Level.INFO, "NoopActivity.call(): Exiting with message {0}",
            CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    return this.message.orElse(INIT_MESSAGE).get();
  }

  @Override
  public synchronized Optional<ActivityMessage> getMessage() {
    LOG.log(Level.INFO, "NoopActivity.getMessage() invoked: {0}",
            CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    return this.message;
  }

  private synchronized void stopActivity() {
    LOG.info("NoopActivity.stopActivity() invoked.");
    this.isRunning = false;
    this.notify();
  }

  public class ActivitySuspendHandler implements EventHandler<SuspendEvent> {
    @Override
    public void onNext(final SuspendEvent suspendEvent) {
      LOG.info("NoopActivity.ActivitySuspendHandler.onNext() invoked.");
      NoopActivity.this.stopActivity();
    }
  }

  public class ActivityStopHandler implements EventHandler<ActivityStop> {
    @Override
    public void onNext(final ActivityStop event) {
      LOG.info("NoopActivity.ActivityStopHandler.onNext() invoked.");
      NoopActivity.this.stopActivity();
    }
  }

  public class ActivityCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOG.info("NoopActivity.ActivityCloseHandler.onNext() invoked.");
      NoopActivity.this.stopActivity();
    }
  }

  public class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(DriverMessage driverMessage) {
      final byte[] msg = driverMessage.get().get();
      LOG.log(Level.INFO, "NoopActivity.DriverMessageHandler.onNext() invoked: {0}", CODEC.decode(msg));
      synchronized (NoopActivity.this) {
        NoopActivity.this.message = Optional.of(ActivityMessage.from(NoopActivity.this.toString(), msg));
      }
    }
  }
}
