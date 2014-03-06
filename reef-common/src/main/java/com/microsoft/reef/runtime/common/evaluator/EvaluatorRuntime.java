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
package com.microsoft.reef.runtime.common.evaluator;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol.EvaluatorControlProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.EvaluatorStatusProto;
import com.microsoft.reef.runtime.common.evaluator.context.ContextManager;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
@EvaluatorSide
final class EvaluatorRuntime {

  private final static Logger LOG = Logger.getLogger(EvaluatorRuntime.class.getName());

  private final String evaluatorIdentifier;

  private final ContextManager contextManager;

  private final HeartBeatManager heartBeatManager;

  private final AutoCloseable evaluatorControlChannel;
  private final Clock clock;

  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;

  @Inject
  private EvaluatorRuntime(final Clock clock, final HeartBeatManager.HeartbeatAlarmHandler heartbeatAlarmHandler,
                           final RemoteManager remoteManager,
                           final ContextManager contextManagerFuture,
                           final HeartBeatManager heartBeatManager,
                           final @Parameter(EvaluatorConfigurationModule.EvaluatorIdentifier.class) String evaluatorIdentifier,
                           final @Parameter(EvaluatorConfigurationModule.DriverRemoteIdentifier.class) String driverRID) {

    this.heartBeatManager = heartBeatManager;
    this.contextManager = contextManagerFuture;
    this.clock = clock;

    this.evaluatorIdentifier = evaluatorIdentifier;
    this.evaluatorControlChannel = remoteManager.registerHandler(driverRID, EvaluatorControlProto.class, new EventHandler<EvaluatorControlProto>() {
      @Override
      public void onNext(final EvaluatorControlProto value) {
        EvaluatorRuntime.this.handle(value);
      }
    });

    /* start the heartbeats */
    clock.scheduleAlarm(0, heartbeatAlarmHandler);
  }

  private void handle(final EvaluatorControlProto message) {
    synchronized (this.heartBeatManager) {
      LOG.log(Level.FINEST, "Evaluator control message");

      if (!message.getIdentifier().equals(this.evaluatorIdentifier.toString())) {
        this.handle(new RuntimeException(
            "Identifier mismatch: message for evaluator id[" + message.getIdentifier()
                + "] sent to evaluator id[" + this.evaluatorIdentifier + "]"));
      } else if (ReefServiceProtos.State.RUNNING != this.state) {
        this.handle(new RuntimeException(
            "Evaluator sent a control message but its state is not "
                + ReefServiceProtos.State.RUNNING + " but rather " + this.state));
      } else {

        if (message.hasContextControl()) {
          LOG.log(Level.FINEST, "Send task control message to ContextManager");
          try {
            this.contextManager.handleContextControlProtocol(message.getContextControl());
            if (this.contextManager.contextStackIsEmpty() && this.state == ReefServiceProtos.State.RUNNING) {
              this.state = ReefServiceProtos.State.DONE;
              this.heartBeatManager.onNext(this.getEvaluatorStatus());
            }
          } catch (Throwable e) {
            this.handle(e);
            throw new RuntimeException(e);
          }
        }

        if (message.hasKillEvaluator()) {
          LOG.log(Level.SEVERE, "Evaluator {0} has been killed by the driver.", this.evaluatorIdentifier);
          this.state = ReefServiceProtos.State.KILLED;
          this.clock.close();
        }
      }
    }
  }

  private final void handle(final Throwable exception) {
    synchronized (this.heartBeatManager) {
      this.state = ReefServiceProtos.State.FAILED;

      final ObjectSerializableCodec<Throwable> codec = new ObjectSerializableCodec<>();
      final EvaluatorStatusProto evaluatorStatusProto = EvaluatorStatusProto.newBuilder()
          .setEvaluatorId(this.evaluatorIdentifier.toString())
          .setError(ByteString.copyFrom(codec.encode(exception)))
          .setState(this.state)
          .build();
      this.heartBeatManager.onNext(evaluatorStatusProto);
      this.contextManager.close();
    }
  }

  public EvaluatorStatusProto getEvaluatorStatus() {
    synchronized (this.heartBeatManager) {
      LOG.log(Level.FINEST, "Evaluator heartbeat: state = {0}", this.state);
      final EvaluatorStatusProto.Builder evaluatorStatus =
          EvaluatorStatusProto.newBuilder()
              .setEvaluatorId(this.evaluatorIdentifier.toString())
              .setState(this.state);
      return evaluatorStatus.build();
    }
  }

  final ReefServiceProtos.State getState() {
    return this.state;
  }

  final class RuntimeStartHandler implements EventHandler<RuntimeStart> {

    @Override
    public final void onNext(final RuntimeStart runtimeStart) {
      synchronized (EvaluatorRuntime.this.heartBeatManager) {
        try {
          LOG.log(Level.FINEST, "runtime start");
          assert (ReefServiceProtos.State.INIT == EvaluatorRuntime.this.state);
          EvaluatorRuntime.this.state = ReefServiceProtos.State.RUNNING;
          EvaluatorRuntime.this.contextManager.start();
          EvaluatorRuntime.this.heartBeatManager.onNext();
        } catch (final Throwable e) {
          EvaluatorRuntime.this.handle(e);
        }
      }
    }
  }

  final class RuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public final void onNext(final RuntimeStop runtimeStop) {
      synchronized (EvaluatorRuntime.this.heartBeatManager) {
        LOG.log(Level.FINEST, "runtime stop");
        EvaluatorRuntime.this.contextManager.close();

        if (ReefServiceProtos.State.RUNNING == EvaluatorRuntime.this.state) {
          throw new RuntimeException("RuntimeStopHandler invoked in state RUNNING.");
        }

        try {
          EvaluatorRuntime.this.evaluatorControlChannel.close();
        } catch (Exception e) {
          LOG.log(Level.SEVERE, "Exception during shutdown of evaluatorControlChannel.", e);
        }
        LOG.log(Level.FINEST, "EvaluatorRuntime shutdown complete");
      }
    }
  }
}
