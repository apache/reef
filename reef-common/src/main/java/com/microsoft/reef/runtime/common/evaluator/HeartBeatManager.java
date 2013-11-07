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

import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.evaluator.context.ContextManager;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class HeartBeatManager {
  private static final Logger LOG = Logger.getLogger(HeartBeatManager.class.getName());

  private final Clock clock;
  private final int heartbeatPeriod;
  private final EventHandler<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatHandler;

  private final InjectionFuture<EvaluatorRuntime> evaluatorRuntime;

  private final InjectionFuture<ContextManager> contextManager;

  @Inject
  private HeartBeatManager(final InjectionFuture<EvaluatorRuntime> evaluatorRuntime,
                           final InjectionFuture<ContextManager> contextManager,
                           final Clock clock,
                           final RemoteManager remoteManager,
                           final @Parameter(EvaluatorConfigurationModule.HeartbeatPeriod.class) int heartbeatPeriod,
                           final @Parameter(EvaluatorConfigurationModule.DriverRemoteIdentifier.class) String driverRID) {
    this.evaluatorRuntime = evaluatorRuntime;
    this.contextManager = contextManager;
    this.clock = clock;
    this.heartbeatPeriod = heartbeatPeriod;
    this.evaluatorHeartbeatHandler = remoteManager.getHandler(driverRID, EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.class);
  }

  /**
   * Assemble a complete new heartbeat and send it out.
   */
  public void onNext() {
    synchronized (this) {
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto = this.getEvaluatorHeartbeatProto();
      LOG.info("heartbeat: " + heartbeatProto);
      this.evaluatorHeartbeatHandler.onNext(heartbeatProto);
    }
  }

  /**
   * Called with a specific ActivityStatus that must be delivered to the driver
   *
   * @param activityStatusProto
   * @return
   */
  public void onNext(final ReefServiceProtos.ActivityStatusProto activityStatusProto) {
    synchronized (this) {
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto = this.getEvaluatorHeartbeatProto(
          this.evaluatorRuntime.get().getEvaluatorStatus(),
          this.contextManager.get().getContextStatusCollection(),
          Optional.of(activityStatusProto));
      LOG.info("heartbeat: " + heartbeatProto);
      this.evaluatorHeartbeatHandler.onNext(heartbeatProto);
    }
  }

  /**
   * Called with a specific ActivityStatus that must be delivered to the driver
   *
   * @param contextStatusProto
   * @return
   */
  public void onNext(final ReefServiceProtos.ContextStatusProto contextStatusProto) {
    synchronized (this) {
      // TODO: Write a test that checks for the order.
      final ArrayList<ReefServiceProtos.ContextStatusProto> contextStatusList = new ArrayList<>();
      contextStatusList.add(contextStatusProto);
      contextStatusList.addAll(this.contextManager.get().getContextStatusCollection());
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto = this.getEvaluatorHeartbeatProto(
          this.evaluatorRuntime.get().getEvaluatorStatus(),
          contextStatusList,
          Optional.<ReefServiceProtos.ActivityStatusProto>empty());
      LOG.info("heartbeat: " + heartbeatProto);
      this.evaluatorHeartbeatHandler.onNext(heartbeatProto);
    }
  }


  /**
   * Called with a specific EvaluatorStatus that must be delivered to the driver
   *
   * @param evaluatorStatusProto
   */
  public void onNext(final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto) {
    synchronized (this) {
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto =
          EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.newBuilder()
              .setTimestamp(System.currentTimeMillis())
              .setEvaluatorStatus(evaluatorStatusProto)
              .build();
      LOG.info("heartbeat: " + heartbeatProto);
      this.evaluatorHeartbeatHandler.onNext(heartbeatProto);
    }
  }


  private EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto getEvaluatorHeartbeatProto() {
    return this.getEvaluatorHeartbeatProto(
        this.evaluatorRuntime.get().getEvaluatorStatus(),
        this.contextManager.get().getContextStatusCollection(),
        this.contextManager.get().getActivityStatus());
  }

  private final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto getEvaluatorHeartbeatProto(
      final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto,
      final Collection<ReefServiceProtos.ContextStatusProto> contextStatusProtos,
      final Optional<ReefServiceProtos.ActivityStatusProto> activityStatusProto) {

    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.Builder builder = EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.newBuilder()
        .setTimestamp(System.currentTimeMillis())
        .setEvaluatorStatus(evaluatorStatusProto);

    for (final ReefServiceProtos.ContextStatusProto contextStatusProto : contextStatusProtos) {
      builder.addContextStatus(contextStatusProto);
    }

    if (activityStatusProto.isPresent()) {
      builder.setActivityStatus(activityStatusProto.get());
    }
    return builder.build();
  }


  final class HeartbeatAlarmHandler implements EventHandler<Alarm> {
    @Override
    public void onNext(final Alarm alarm) {
      synchronized (HeartBeatManager.this) {
        if (HeartBeatManager.this.evaluatorRuntime.get().getState() == ReefServiceProtos.State.RUNNING) {
          final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto evaluatorHeartbeatProto = HeartBeatManager.this.getEvaluatorHeartbeatProto();
          LOG.log(Level.FINE, "Triggering a heartbeat:\n" + evaluatorHeartbeatProto.toString());
          HeartBeatManager.this.evaluatorHeartbeatHandler.onNext(evaluatorHeartbeatProto);
          HeartBeatManager.this.clock.scheduleAlarm(HeartBeatManager.this.heartbeatPeriod, this);
        } else {
          LOG.log(Level.FINE, "Not triggering a heartbeat, because state is:" + HeartBeatManager.this.evaluatorRuntime.get().getState());
        }
      }
    }
  }

}
