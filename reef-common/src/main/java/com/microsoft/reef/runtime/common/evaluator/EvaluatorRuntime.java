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
package com.microsoft.reef.runtime.common.evaluator;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.EvaluatorSide;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol.EvaluatorControlProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.EvaluatorStatusProto;
import com.microsoft.reef.runtime.common.evaluator.context.ContextManager;
import com.microsoft.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import com.microsoft.reef.runtime.common.evaluator.parameters.EvaluatorIdentifier;
import com.microsoft.reef.runtime.common.evaluator.parameters.HeartbeatPeriod;
import com.microsoft.reef.runtime.common.utils.ExceptionCodec;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
@EvaluatorSide
final class EvaluatorRuntime implements EventHandler<EvaluatorControlProto> {

  private final static Logger LOG = Logger.getLogger(EvaluatorRuntime.class.getName());

  private final HeartBeatManager heartBeatManager;
  private final ContextManager contextManager;
  private final Clock clock;

  private final String evaluatorIdentifier;
  private final ExceptionCodec exceptionCodec;
  private final AutoCloseable evaluatorControlChannel;

  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;

  @Inject
  private EvaluatorRuntime(
      final @Parameter(HeartbeatPeriod.class) int heartbeatPeriod,
      final @Parameter(EvaluatorIdentifier.class) String evaluatorIdentifier,
      final @Parameter(DriverRemoteIdentifier.class) String driverRID,
      final HeartBeatManager.HeartbeatAlarmHandler heartbeatAlarmHandler,
      final HeartBeatManager heartBeatManager,
      final Clock clock,
      final ContextManager contextManagerFuture,
      final RemoteManager remoteManager,
      final ExceptionCodec exceptionCodec) {

    this.heartBeatManager = heartBeatManager;
    this.contextManager = contextManagerFuture;
    this.clock = clock;

    this.evaluatorIdentifier = evaluatorIdentifier;
    this.exceptionCodec = exceptionCodec;
    this.evaluatorControlChannel =
        remoteManager.registerHandler(driverRID, EvaluatorControlProto.class, this);

    // start the heartbeats
    clock.scheduleAlarm(heartbeatPeriod, heartbeatAlarmHandler);
  }

  private void onEvaluatorControlMessage(final EvaluatorControlProto message) {

    synchronized (this.heartBeatManager) {

      LOG.log(Level.FINEST, "Evaluator control message");

      if (!message.getIdentifier().equals(this.evaluatorIdentifier)) {
        this.onException(new RuntimeException(
            "Identifier mismatch: message for evaluator id[" + message.getIdentifier()
                + "] sent to evaluator id[" + this.evaluatorIdentifier + "]"
        ));
      } else if (ReefServiceProtos.State.RUNNING != this.state) {
        this.onException(new RuntimeException(
            "Evaluator sent a control message but its state is not "
                + ReefServiceProtos.State.RUNNING + " but rather " + this.state
        ));
      } else {

        if (message.hasContextControl()) {

          LOG.log(Level.FINEST, "Send task control message to ContextManager");

          try {
            this.contextManager.handleContextControlProtocol(message.getContextControl());
            if (this.contextManager.contextStackIsEmpty() && this.state == ReefServiceProtos.State.RUNNING) {
              this.state = ReefServiceProtos.State.DONE;
              this.heartBeatManager.sendEvaluatorStatus(this.getEvaluatorStatus());
              this.clock.close();
            }
          } catch (final Throwable e) {
            this.onException(e);
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

  private final void onException(final Throwable exception) {
    synchronized (this.heartBeatManager) {
      this.state = ReefServiceProtos.State.FAILED;

      final EvaluatorStatusProto evaluatorStatusProto = EvaluatorStatusProto.newBuilder()
          .setEvaluatorId(this.evaluatorIdentifier)
          .setError(ByteString.copyFrom(this.exceptionCodec.toBytes(exception)))
          .setState(this.state)
          .build();
      this.heartBeatManager.sendEvaluatorStatus(evaluatorStatusProto);
      this.contextManager.close();
    }
  }

  public EvaluatorStatusProto getEvaluatorStatus() {
    synchronized (this.heartBeatManager) {
      LOG.log(Level.FINEST, "Evaluator heartbeat: state = {0}", this.state);
      final EvaluatorStatusProto.Builder evaluatorStatus =
          EvaluatorStatusProto.newBuilder()
              .setEvaluatorId(this.evaluatorIdentifier)
              .setState(this.state);
      return evaluatorStatus.build();
    }
  }

  final ReefServiceProtos.State getState() {
    return this.state;
  }

  boolean isRunning() {
    return this.state == ReefServiceProtos.State.RUNNING;
  }

  @Override
  public void onNext(EvaluatorControlProto evaluatorControlProto) {
    this.onEvaluatorControlMessage(evaluatorControlProto);
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
          EvaluatorRuntime.this.heartBeatManager.sendHeartbeat();
        } catch (final Throwable e) {
          EvaluatorRuntime.this.onException(e);
        }
      }
    }
  }

  final class RuntimeStopHandler implements EventHandler<RuntimeStop> {

    @Override
    public final void onNext(final RuntimeStop runtimeStop) {
      synchronized (EvaluatorRuntime.this.heartBeatManager) {
        LOG.log(Level.FINEST, "EvaluatorRuntime shutdown invoked for Evaluator {0} in state {1}",
            new Object[]{evaluatorIdentifier, state});

        if (EvaluatorRuntime.this.isRunning()) {
          EvaluatorRuntime.this.onException(new RuntimeException(
              "RuntimeStopHandler invoked in state RUNNING.", runtimeStop.getException()));
        } else {
          EvaluatorRuntime.this.contextManager.close();
          try {
            EvaluatorRuntime.this.evaluatorControlChannel.close();
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Exception during shutdown of evaluatorControlChannel.", e);
          }
          LOG.log(Level.FINEST, "EvaluatorRuntime shutdown complete");
        }
      }
    }
  }
}
