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

package org.apache.reef.bridge.driver.common.grpc;

import com.google.protobuf.ByteString;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.proto.ContextInfo;
import org.apache.reef.bridge.proto.EvaluatorDescriptorInfo;
import org.apache.reef.bridge.proto.ExceptionInfo;
import org.apache.reef.driver.context.ContextBase;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.util.Optional;

/**
 * Utility methods for gRPC.
 */
@Private
public final class GRPCUtils {

  private GRPCUtils() {
  }

  /**
   * Converts ByteString to byte array.
   * @param bs ByteString
   * @return byte array or null if not present
   */
  public static byte[] toByteArray(final ByteString bs) {
    return bs == null || bs.isEmpty() ? null : bs.toByteArray();
  }

  /**
   * Converts ByteString into an optional byte array.
   * @param bs ByteString object
   * @return Optional of byte array
   */
  public static Optional<byte[]> toByteArrayOptional(final ByteString bs) {
    return Optional.ofNullable(toByteArray(bs));
  }

  /**
   * Create exception info from exception object.
   * @param exceptionCodec to encode exception into bytes
   * @param ex exception object
   * @return ExceptionInfo
   */
  public static ExceptionInfo createExceptionInfo(final ExceptionCodec exceptionCodec, final Throwable ex)  {
    return ExceptionInfo.newBuilder()
        .setName(ex.getCause() != null ? ex.getCause().toString() : ex.toString())
        .setMessage(ex.getMessage() == null ? ex.toString() : ex.getMessage())
        .setData(ByteString.copyFrom(exceptionCodec.toBytes(ex)))
        .build();
  }

  /**
   * Create an evaluator descriptor info from an EvalautorDescriptor object.
   * @param descriptor object
   * @return EvaluatorDescriptorInfo
   */
  public static EvaluatorDescriptorInfo toEvaluatorDescriptorInfo(
      final EvaluatorDescriptor descriptor) {
    return descriptor == null ? null : EvaluatorDescriptorInfo.newBuilder()
        .setCores(descriptor.getNumberOfCores())
        .setMemory(descriptor.getMemory())
        .setRuntimeName(descriptor.getRuntimeName())
        .build();
  }

  /**
   * Create a context info from a context object.
   * @param context object
   * @return context info
   */
  public static ContextInfo toContextInfo(final ContextBase context) {
    return toContextInfo(context, null);
  }

  /**
   * Create a context info from a context object with an error.
   * @param context object
   * @param error info
   * @return context info
   */
  public static ContextInfo toContextInfo(final ContextBase context, final ExceptionInfo error) {
    final ContextInfo.Builder builder = ContextInfo.newBuilder()
        .setContextId(context.getId())
        .setEvaluatorId(context.getEvaluatorId())
        .setParentId(context.getParentId().orElse(""))
        .setEvaluatorDescriptorInfo(toEvaluatorDescriptorInfo(
            context.getEvaluatorDescriptor()));
    if (error != null) {
      builder.setException(error);
    }
    return builder.build();
  }

}
