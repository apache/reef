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

package com.microsoft.reef.tests.messaging.activity;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic activity that receives a message and sends it back to the driver.
 */
@Unit
public final class ActivityMsg implements Activity, ActivityMessageSource {

  private static final Logger LOG = Logger.getLogger(ActivityMsg.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final ActivityMessage INIT_MESSAGE = ActivityMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient boolean isRunning = true;
  private transient Optional<ActivityMessage> message = Optional.empty();

  @Inject
  public ActivityMsg() {
    LOG.info("ActivityMsg created.");
  }

  @Override
  public synchronized byte[] call(final byte[] memento) {
    LOG.info("ActivityMsg.call() invoked. Waiting for the message.");
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
  public synchronized Optional<ActivityMessage> getMessage() {
    LOG.log(Level.INFO, "ActivityMsg.getMessage() invoked: {0}",
            CODEC.decode(this.message.orElse(INIT_MESSAGE).get()));
    if (this.message.isPresent()) {
      this.isRunning = false;
      this.notify();
    }
    return this.message;
  }

  public class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(DriverMessage driverMessage) {
      final byte[] message = driverMessage.get().get();
      LOG.log(Level.INFO, "ActivityMsg.onNext() invoked: {0}", CODEC.decode(message));
      ActivityMsg.this.message = Optional.of(ActivityMessage.from(this.toString(), message));
    }
  }
}
