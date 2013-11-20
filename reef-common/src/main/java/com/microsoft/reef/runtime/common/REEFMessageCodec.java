package com.microsoft.reef.runtime.common;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.EvaluatorRuntimeProtocol;
import com.microsoft.reef.proto.REEFProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Codec for REEF's control flow messages.
 */
public final class REEFMessageCodec implements Codec<GeneratedMessage> {


  @Inject
  private REEFMessageCodec() {
    Logger.getLogger(REEFMessageCodec.class.getName()).log(Level.INFO, "REEFMessageCodec instantiated.");
  }

  @Override
  public GeneratedMessage decode(final byte[] bytes) {
    try {
      final REEFProtocol.REEFMessage message = REEFProtocol.REEFMessage.parseFrom(bytes);
      if (message.hasJobSubmission()) {
        return message.getJobSubmission();
      } else if (message.hasJobControl()) {
        return message.getJobControl();
      } else if (message.hasRuntimeError()) {
        return message.getRuntimeError();
      } else if (message.hasJobStatus()) {
        return message.getJobStatus();
      } else if (message.hasEvaluatorControl()) {
        return message.getEvaluatorControl();
      } else if (message.hasEvaluatorHeartBeat()) {
        return message.getEvaluatorHeartBeat();
      }
      throw new RuntimeException("Unable to decode a message: " + message.toString());
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to decode a message", e);
    }
  }

  @Override
  public byte[] encode(final GeneratedMessage msg) {
    final REEFProtocol.REEFMessage.Builder message = REEFProtocol.REEFMessage.newBuilder();

    if (msg instanceof ClientRuntimeProtocol.JobSubmissionProto) {
      message.setJobSubmission((ClientRuntimeProtocol.JobSubmissionProto) msg);
    } else if (msg instanceof ClientRuntimeProtocol.JobControlProto) {
      message.setJobControl((ClientRuntimeProtocol.JobControlProto) msg);
    } else if (msg instanceof ReefServiceProtos.RuntimeErrorProto) {
      message.setRuntimeError((ReefServiceProtos.RuntimeErrorProto) msg);
    } else if (msg instanceof ReefServiceProtos.JobStatusProto) {
      message.setJobStatus((ReefServiceProtos.JobStatusProto) msg);
    } else if (msg instanceof EvaluatorRuntimeProtocol.EvaluatorControlProto) {
      message.setEvaluatorControl((EvaluatorRuntimeProtocol.EvaluatorControlProto) msg);
    } else if (msg instanceof EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto) {
      message.setEvaluatorHeartBeat((EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto) msg);
    } else {
      throw new RuntimeException("Unable to serialize: " + msg);
    }

    return message.build().toByteArray();
  }
}
