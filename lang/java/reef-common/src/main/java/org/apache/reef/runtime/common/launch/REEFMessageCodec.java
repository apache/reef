/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.common.launch;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.proto.ClientRuntimeProtocol;
import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.proto.EvaluatorShimProtocol;
import org.apache.reef.proto.REEFProtocol;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Codec for REEF's control flow messages.
 */
@Private
@Provided
@ClientSide
@DriverSide
@EvaluatorSide
public final class REEFMessageCodec implements Codec<GeneratedMessage> {


  @Inject
  private REEFMessageCodec() {
  }

  @Override
  public GeneratedMessage decode(final byte[] bytes) {
    try {
      final REEFProtocol.REEFMessage message = REEFProtocol.REEFMessage.parseFrom(bytes);
      if (message.hasJobControl()) {
        return message.getJobControl();
      } else if (message.hasRuntimeError()) {
        return message.getRuntimeError();
      } else if (message.hasJobStatus()) {
        return message.getJobStatus();
      } else if (message.hasEvaluatorControl()) {
        return message.getEvaluatorControl();
      } else if (message.hasEvaluatorHeartBeat()) {
        return message.getEvaluatorHeartBeat();
      } else if (message.hasEvaluatorShimCommand()) {
        return message.getEvaluatorShimCommand();
      } else if (message.hasEvaluatorShimStatus()) {
        return message.getEvaluatorShimStatus();
      }
      throw new RuntimeException("Unable to decode a message: " + message.toString());
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to decode a message", e);
    }
  }

  @Override
  public byte[] encode(final GeneratedMessage msg) {
    final REEFProtocol.REEFMessage.Builder message = REEFProtocol.REEFMessage.newBuilder();

    if (msg instanceof ClientRuntimeProtocol.JobControlProto) {
      message.setJobControl((ClientRuntimeProtocol.JobControlProto) msg);
    } else if (msg instanceof ReefServiceProtos.RuntimeErrorProto) {
      message.setRuntimeError((ReefServiceProtos.RuntimeErrorProto) msg);
    } else if (msg instanceof ReefServiceProtos.JobStatusProto) {
      message.setJobStatus((ReefServiceProtos.JobStatusProto) msg);
    } else if (msg instanceof EvaluatorRuntimeProtocol.EvaluatorControlProto) {
      message.setEvaluatorControl((EvaluatorRuntimeProtocol.EvaluatorControlProto) msg);
    } else if (msg instanceof EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto) {
      message.setEvaluatorHeartBeat((EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto) msg);
    } else if (msg instanceof EvaluatorShimProtocol.EvaluatorShimControlProto) {
      message.setEvaluatorShimCommand((EvaluatorShimProtocol.EvaluatorShimControlProto) msg);
    } else if (msg instanceof EvaluatorShimProtocol.EvaluatorShimStatusProto) {
      message.setEvaluatorShimStatus((EvaluatorShimProtocol.EvaluatorShimStatusProto) msg);
    } else {
      throw new RuntimeException("Unable to serialize: " + msg);
    }

    return message.build().toByteArray();
  }
}
