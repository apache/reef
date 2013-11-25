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

import com.google.protobuf.ByteString;
import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.ActivityMessageSource;
import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
import com.microsoft.reef.driver.context.ContextConfigurationOptions;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.HeartBeatManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the various states an Activity could be in.
 */
public final class ActivityStatus {
  private static final Logger LOG = Logger.getLogger(ActivityStatus.class.getName());

  private final String activityID;
  private final String contextID;
  private Optional<Throwable> lastException = Optional.empty();
  private Optional<byte[]> result = Optional.empty();
  private State state;

  private final ActivityLifeCycle activityLifeCycle;
  private final HeartBeatManager heartBeatManager;
  private final Set<ActivityMessageSource> evaluatorMessageSources;


  enum State {
    INIT,
    RUNNING,
    CLOSE_REQUESTED,
    SUSPEND_REQUESTED,
    SUSPENDED,
    FAILED,
    DONE,
    KILLED
  }

  @Inject
  ActivityStatus(final @Parameter(ActivityConfigurationOptions.Identifier.class) String activityID,
                 final @Parameter(ContextConfigurationOptions.ContextIdentifier.class) String contextID,
                 final @Parameter(ActivityConfigurationOptions.ActivityMessageSources.class) Set<ActivityMessageSource> evaluatorMessageSources,
                 final ActivityLifeCycle activityLifeCycle,
                 final HeartBeatManager heartBeatManager) {
    this.activityID = activityID;
    this.contextID = contextID;
    this.activityLifeCycle = activityLifeCycle;
    this.heartBeatManager = heartBeatManager;
    this.evaluatorMessageSources = evaluatorMessageSources;

    this.setState(State.INIT);
  }

  public final String getActivityID() {
    return this.activityID;
  }

  ReefServiceProtos.ActivityStatusProto toProto() {
    this.check();
    final ReefServiceProtos.ActivityStatusProto.Builder result = ReefServiceProtos.ActivityStatusProto.newBuilder()
        .setContextId(this.contextID)
        .setActivityId(this.activityID)
        .setState(this.getProtoState());

    if (this.result.isPresent()) {
      result.setResult(ByteString.copyFrom(this.result.get()));
    } else if (this.lastException.isPresent()) {
      final Encoder<Throwable> codec = new ObjectSerializableCodec<>();
      final byte[] error = codec.encode(this.lastException.get());
      result.setResult(ByteString.copyFrom(error));
    }
    for (final ActivityMessage activityMessage : this.getMessages()) {
      result.addActivityMessage(ReefServiceProtos.ActivityStatusProto.ActivityMessageProto.newBuilder()
          .setSourceId(activityMessage.getMessageSourceID())
          .setMessage(ByteString.copyFrom(activityMessage.get()))
          .build());
    }

    return result.build();
  }

  private void check() {
    if (this.result.isPresent() && this.lastException.isPresent()) {
      throw new RuntimeException("Found both an exception and a result. This is unsupported.");
    }
  }

  private ReefServiceProtos.State getProtoState() {
    switch (this.state) {
      case INIT:
        return ReefServiceProtos.State.INIT;
      case CLOSE_REQUESTED:
      case SUSPEND_REQUESTED:
      case RUNNING:
        return ReefServiceProtos.State.RUNNING;
      case DONE:
        return ReefServiceProtos.State.DONE;
      case SUSPENDED:
        return ReefServiceProtos.State.SUSPEND;
      case FAILED:
        return ReefServiceProtos.State.FAILED;
      case KILLED:
        return ReefServiceProtos.State.KILLED;
    }
    throw new RuntimeException("Unknown state: " + this.state);
  }

  void setException(final Throwable throwable) {
    this.lastException = Optional.of(throwable);
    this.state = State.FAILED;
    this.check();
    this.activityLifeCycle.stop();
    this.heartbeat();
  }

  void setResult(final byte[] result) {
    this.result = Optional.ofNullable(result);
    if (this.state == State.RUNNING) {
      this.setState(State.DONE);
    } else if (this.state == State.SUSPEND_REQUESTED) {
      this.setState(State.SUSPENDED);
    } else if (this.state == State.CLOSE_REQUESTED) {
      this.setState(State.DONE);
    }
    this.check();
    this.activityLifeCycle.stop();
    this.heartbeat();
  }

  private void setState(final State state) {
    if (isLegal(this.state, state)) {
      this.state = state;
    } else {
      final String msg = "Illegal state transition from [" + this.state + "] to [" + state + "]";
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }
  }

  private void heartbeat() {
    this.heartBeatManager.onNext(this.toProto());
  }

  void setRunning() {
    if (this.state == State.INIT) {
      try {
        this.activityLifeCycle.start();

        /* I need to send an INIT heartbeat to the driver
         * prompting it to create an RunningActivity event.
         * RUNNING state heartbeats are not time dependent,
         * and can happen at the next scheduled heartbeat period. */
        this.heartbeat();
        this.setState(State.RUNNING);
      } catch (Exception e) {
        // Activity start handler error
        this.setException(e);
      }
    }
  }

  void setCloseRequested() {
    this.setState(State.CLOSE_REQUESTED);
  }

  void setSuspendRequested() {
    this.setState(State.SUSPEND_REQUESTED);
  }


  void setKilled() {
    this.setState(State.KILLED);
    this.heartbeat();
  }

  boolean isNotRunning() {
    return this.state != State.RUNNING;
  }

  boolean hasEnded() {
    switch (this.state) {
      case DONE:
      case SUSPENDED:
      case FAILED:
      case KILLED:
        return true;
      default:
        return false;
    }
  }

  State getState() {
    return this.state;
  }


  String getContextID() {
    return this.contextID;
  }

  /**
   * @param from
   * @param to
   * @return true, if the state transition from state 'from' to state 'to' is legal.
   */
  private static boolean isLegal(final State from, final State to) {
    if (from == null) {
      return to == State.INIT;
    }
    switch (from) {
      case INIT:
        switch (to) {
          case RUNNING:
          case FAILED:
          case KILLED:
          case DONE:
            return true;
          default:
            return false;
        }
      case RUNNING:
        switch (to) {
          case CLOSE_REQUESTED:
          case SUSPEND_REQUESTED:
          case FAILED:
          case KILLED:
          case DONE:
            return true;
          default:
            return false;
        }
      case CLOSE_REQUESTED:
        switch (to) {
          case FAILED:
          case KILLED:
          case DONE:
            return true;
          default:
            return false;
        }
      case SUSPEND_REQUESTED:
        switch (to) {
          case FAILED:
          case KILLED:
          case SUSPENDED:
            return true;
          default:
            return false;
        }

      case FAILED:
      case DONE:
      case KILLED:
        return false;
      default:
        return false;
    }
  }


  /**
   * @return the messages to be sent on the Activity's behalf in the next heartbeat.
   */
  private final Collection<ActivityMessage> getMessages() {
    final ArrayList<ActivityMessage> result = new ArrayList<>(this.evaluatorMessageSources.size());
    for (final ActivityMessageSource messageSource : this.evaluatorMessageSources) {
      final Optional<ActivityMessage> activityMessageOptional = messageSource.getMessage();
      if (activityMessageOptional.isPresent()) {
        result.add(activityMessageOptional.get());
      }
    }
    return result;
  }
}
