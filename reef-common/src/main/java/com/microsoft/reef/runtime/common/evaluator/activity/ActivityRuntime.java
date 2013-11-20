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
package com.microsoft.reef.runtime.common.evaluator.activity;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.activity.events.CloseEvent;
import com.microsoft.reef.activity.events.DriverMessage;
import com.microsoft.reef.activity.events.SuspendEvent;
import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
import com.microsoft.reef.proto.ReefServiceProtos.ActivityStatusProto;
import com.microsoft.reef.runtime.common.evaluator.HeartBeatManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The runtime environment for an Activity.
 */
@Private
@EvaluatorSide
public final class ActivityRuntime extends Thread {

  private final static Logger LOG = Logger.getLogger(ActivityRuntime.class.getName());

  /// User supplied Activity code
  private final Activity activity;
  private final InjectionFuture<EventHandler<CloseEvent>> f_closeHandler;
  private final InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler;
  private final InjectionFuture<EventHandler<DriverMessage>> f_messageHandler;

  // The memento given by the activity configuration
  private final Optional<byte[]> memento;

  // Heart beat manager to trigger on heartbeats.
  private final HeartBeatManager heartBeatManager;

  private final ActivityStatus currentStatus;

  // TODO: Document
  @Inject
  private ActivityRuntime(final HeartBeatManager heartBeatManager,
                          final Activity activity,
                          final ActivityStatus currentStatus,
                          final @Parameter(ActivityConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
                          final @Parameter(ActivityConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
                          final @Parameter(ActivityConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler) {
    this(heartBeatManager, activity, currentStatus, f_closeHandler, f_suspendHandler, f_messageHandler, null);
  }

  // TODO: Document
  @Inject
  private ActivityRuntime(final HeartBeatManager heartBeatManager,
                          final Activity activity,
                          final ActivityStatus currentStatus,
                          final @Parameter(ActivityConfigurationOptions.CloseHandler.class) InjectionFuture<EventHandler<CloseEvent>> f_closeHandler,
                          final @Parameter(ActivityConfigurationOptions.SuspendHandler.class) InjectionFuture<EventHandler<SuspendEvent>> f_suspendHandler,
                          final @Parameter(ActivityConfigurationOptions.MessageHandler.class) InjectionFuture<EventHandler<DriverMessage>> f_messageHandler,
                          final @Parameter(ActivityConfigurationOptions.Memento.class) String memento) {
    this.heartBeatManager = heartBeatManager;
    this.activity = activity;
    this.memento = null == memento ? Optional.<byte[]>empty() :
        Optional.of(DatatypeConverter.parseBase64Binary(memento));

    this.f_closeHandler = f_closeHandler;
    this.f_suspendHandler = f_suspendHandler;
    this.f_messageHandler = f_messageHandler;

    this.currentStatus = currentStatus;

  }

  // TODO: Document
  public void initialize() {
    this.currentStatus.setRunning();
  }

  /**
   * Run the activity: Fire ActivityStart, call Activity.call(), fire ActivityStop.
   */
  @Override
  public void run() {
    try {
      LOG.info("call activity");
      if (this.currentStatus.isNotRunning()) {
        throw new RuntimeException("ActivityRuntime not initialized!");
      }

      final byte[] result;
      if (this.memento.isPresent()) {
        result = this.activity.call(this.memento.get());
      } else {
        result = this.activity.call(null);
      }

      synchronized(this.heartBeatManager) {
        LOG.info("activity call finished");
        this.currentStatus.setResult(result);
      }

    } catch (final InterruptedException e) {
      synchronized (this.heartBeatManager) {
        LOG.log(Level.WARNING, "Killed the Activity", e);
        this.currentStatus.setKilled();
      }
    } catch (final Throwable throwable) {
      synchronized (this.heartBeatManager) {
        this.currentStatus.setException(throwable);
      }
    }
  }

  /**
   * Called by heartbeat manager
   *
   * @return current ActivityStatusProto
   */
  public final ActivityStatusProto getStatusProto() {
    return this.currentStatus.toProto();
  }

  // TODO: Document
  public final boolean hasEnded() {
    return this.currentStatus.hasEnded();
  }

  /**
   * @return the ID of the activity.
   */
  public String getActivityId() {
    return this.currentStatus.getActivityID();
  }

  /**
   * Close the Activity. This calls the configured close handler.
   *
   * @param message the optional message for the close handler or null if there none.
   */
  public final void close(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to close an activity that is in state: '" + this.currentStatus.getState() + "' . Ignoring.");
      } else {
        try {
          this.f_closeHandler.get().onNext(new CloseEventImpl(message));
          this.currentStatus.setCloseRequested();
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new ActivityClientCodeException(this.getActivityId(), this.getContextID(), "Error during close().", throwable));
        }
      }
    }
  }

  /**
   * Suspend the Activity.  This calls the configured suspend handler.
   *
   * @param message the optional message for the suspend handler or null if there none.
   */
  public final void suspend(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to suspend an activity that is in state: '" + this.currentStatus.getState() + "' . Ignoring.");
      } else {
        try {
          this.f_suspendHandler.get().onNext(new SuspendEventImpl(message));
          this.currentStatus.setSuspendRequested();
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new ActivityClientCodeException(this.getActivityId(), this.getContextID(), "Error during suspend().", throwable));
        }
      }
    }
  }

  /**
   * Deliver a message to the Activity. This calls into the user supplied message handler.
   *
   * @param message the message to be delivered.
   */
  public final void deliver(final byte[] message) {
    synchronized (this.heartBeatManager) {
      if (this.currentStatus.isNotRunning()) {
        LOG.log(Level.WARNING, "Trying to send a message to an activity that is in state: '" + this.currentStatus.getState() + "' . Ignoring.");
      } else {
        try {
          this.f_messageHandler.get().onNext(new DriverMessageImpl(message));
        } catch (final Throwable throwable) {
          this.currentStatus.setException(new ActivityClientCodeException(this.getActivityId(), this.getContextID(), "Error during message delivery.", throwable));
        }
      }
    }
  }

  final String getContextID() {
    return this.currentStatus.getContextID();
  }
}
