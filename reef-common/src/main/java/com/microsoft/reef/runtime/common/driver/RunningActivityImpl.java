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
package com.microsoft.reef.runtime.common.driver;

import com.google.protobuf.ByteString;
import com.microsoft.reef.driver.activity.RunningActivity;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol.ContextControlProto;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol.StopActivityProto;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol.SuspendActivityProto;

import java.util.logging.Logger;

/**
 * Implements the ActivityRuntime client interface. It is mainly a helper class
 * that will package up various client method calls into protocol buffers and
 * pass them to its respective EvaluatorManager to deliver to the EvaluatorRuntime.
 */
final class RunningActivityImpl implements RunningActivity {

  private final static Logger LOG = Logger.getLogger(RunningActivity.class.getName());

  private final EvaluatorManager evaluatorManager;
  private final EvaluatorContext evaluatorContext;
  private final String activityId;

  RunningActivityImpl(final EvaluatorManager evaluatorManager, final String activityId, final EvaluatorContext evaluatorContext) {
    LOG.info("INIT: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    this.evaluatorManager = evaluatorManager;
    this.evaluatorContext = evaluatorContext;
    this.activityId = activityId;
  }


  @Override
  public ActiveContext getActiveContext() {
    return this.evaluatorContext;
  }

  @Override
  public String getId() {
    return this.activityId;
  }

  @Override
  public final void onNext(final byte[] message) {
    LOG.info("MESSAGE: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    ContextControlProto contextControlProto = ContextControlProto.newBuilder()
            .setActivityMessage(ByteString.copyFrom(message))
            .build();

    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public void close() {
    LOG.info("CLOSE: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    ContextControlProto contextControlProto = ContextControlProto.newBuilder()
            .setStopActivity(StopActivityProto.newBuilder().build())
            .build();
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public void close(byte[] message) {
    LOG.info("CLOSE: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "] with message.");

    ContextControlProto contextControlProto = ContextControlProto.newBuilder()
            .setStopActivity(StopActivityProto.newBuilder().build())
            .setActivityMessage(ByteString.copyFrom(message))
            .build();
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public void suspend(byte[] message) {
    LOG.info("SUSPEND: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "] with message.");

    ContextControlProto contextControlProto = ContextControlProto.newBuilder()
            .setSuspendActivity(SuspendActivityProto.newBuilder().build())
            .setActivityMessage(ByteString.copyFrom(message))
            .build();
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public void suspend() {
    LOG.info("SUSPEND: ActivityRuntime id[" + activityId + "] on evaluator id[" + evaluatorManager.getId() + "]");

    ContextControlProto contextControlProto = ContextControlProto.newBuilder()
            .setSuspendActivity(SuspendActivityProto.newBuilder().build())
            .build();
    this.evaluatorManager.handle(contextControlProto);
  }

  @Override
  public String toString() {
    return "ActivityRuntime{activityId='" + activityId + "'}";
  }
}
