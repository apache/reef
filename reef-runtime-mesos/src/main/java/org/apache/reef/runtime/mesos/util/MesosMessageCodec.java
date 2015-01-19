/**
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
package org.apache.reef.runtime.mesos.util;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.EvaluatorLaunchProto;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.EvaluatorReleaseProto;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.REEFRuntimeMesosMessage;
import org.apache.reef.runtime.mesos.proto.ReefRuntimeMesosProtocol.REEFRuntimeMesosMessage.Builder;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * MesssageCodec for MesosRemoteManager.
 * TODO: Replace this class once Tang's namespace feature is enabled
 */
@Private
@Provided
@DriverSide
@EvaluatorSide
public final class MesosMessageCodec implements Codec<GeneratedMessage> {
  @Inject
  public MesosMessageCodec() {
  }

  @Override
  public GeneratedMessage decode(final byte[] bytes) {
    try {
      final REEFRuntimeMesosMessage message = REEFRuntimeMesosMessage.parseFrom(bytes);
      if (message.hasEvaluatorLaunch()) {
        return message.getEvaluatorLaunch();
      } else if (message.hasEvaluatorRelease()) {
        return message.getEvaluatorRelease();
      } else {
        throw new RuntimeException("Unable to decode a message: " + message.toString());
      }
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to decode a message", e);
    }
  }

  @Override
  public byte[] encode(final GeneratedMessage msg) {
    final Builder message = REEFRuntimeMesosMessage.newBuilder();

    if (msg instanceof EvaluatorLaunchProto) {
      message.setEvaluatorLaunch((EvaluatorLaunchProto) msg);
    } else if (msg instanceof EvaluatorReleaseProto) {
      message.setEvaluatorRelease((EvaluatorReleaseProto) msg);
    } else {
      throw new RuntimeException("Unable to serialize: " + msg);
    }

    return message.build().toByteArray();
  }
}