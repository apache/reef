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
package org.apache.reef.io.watcher.util;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.common.Failure;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextBase;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.io.watcher.common.AvroFailure;
import org.apache.reef.io.watcher.driver.catalog.AvroNodeDescriptor;
import org.apache.reef.io.watcher.driver.catalog.AvroNodeDescriptorInRackDescriptor;
import org.apache.reef.io.watcher.driver.catalog.AvroRackDescriptor;
import org.apache.reef.io.watcher.driver.context.AvroActiveContext;
import org.apache.reef.io.watcher.driver.context.AvroClosedContext;
import org.apache.reef.io.watcher.driver.context.AvroContextBase;
import org.apache.reef.io.watcher.driver.context.AvroFailedContext;
import org.apache.reef.io.watcher.driver.evaluator.*;
import org.apache.reef.io.watcher.driver.task.*;
import org.apache.reef.io.watcher.wake.time.event.AvroStartTime;
import org.apache.reef.io.watcher.wake.time.event.AvroStopTime;
import org.apache.reef.io.watcher.wake.time.runtime.event.AvroRuntimeStart;
import org.apache.reef.io.watcher.wake.time.runtime.event.AvroRuntimeStop;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@Private
@Unstable
public final class WatcherAvroUtil {

  public static AvroFailure toAvroFailure(final Failure failure) {
    final String reason;
    if (failure.getReason().isPresent()) {
      reason = convertThrowableToString(failure.getReason().get());
    } else {
      reason = null;
    }

    return AvroFailure.newBuilder()
        .setAsError(convertThrowableToString(failure.asError()))
        .setData(unwrapOptionalByteArray(failure.getData()))
        .setDescription(failure.getDescription().orElse(null))
        .setId(failure.getId())
        .setMessage(failure.getMessage())
        .setReason(reason)
        .build();
  }

  public static AvroNodeDescriptorInRackDescriptor toAvroNodeDescriptorInRackDescriptor(
      final String id, final String name, final InetSocketAddress inetSocketAddress) {
    return AvroNodeDescriptorInRackDescriptor.newBuilder()
        .setInetSocketAddress(inetSocketAddress.toString())
        .setId(id)
        .setName(name)
        .build();
  }

  public static AvroRackDescriptor toAvroRackDescriptor(final RackDescriptor rackDescriptor) {
    final List<AvroNodeDescriptorInRackDescriptor> nodeDescriptorList = new ArrayList<>();
    for (final NodeDescriptor nodeDescriptor : rackDescriptor.getNodes()) {
      nodeDescriptorList.add(
          toAvroNodeDescriptorInRackDescriptor(
              nodeDescriptor.getId(), nodeDescriptor.getName(), nodeDescriptor.getInetSocketAddress()
          )
      );
    }

    return AvroRackDescriptor.newBuilder()
        .setNodes(nodeDescriptorList)
        .setName(rackDescriptor.getName())
        .build();
  }

  public static AvroNodeDescriptor toAvroNodeDescriptor(final NodeDescriptor nodeDescriptor) {
    return AvroNodeDescriptor.newBuilder()
        .setId(nodeDescriptor.getId())
        .setName(nodeDescriptor.getName())
        .setInetSocketAddress(nodeDescriptor.getInetSocketAddress().toString())
        .setRackDescriptor(toAvroRackDescriptor(nodeDescriptor.getRackDescriptor()))
        .build();
  }

  public static AvroEvaluatorType toAvroEvaluatorType(final EvaluatorType evaluatorType) {
    switch (evaluatorType) {
    case JVM: return AvroEvaluatorType.JVM;
    case CLR: return AvroEvaluatorType.CLR;
    case UNDECIDED: return AvroEvaluatorType.UNDECIDED;
    default: throw new RuntimeException(evaluatorType + " is not defined for AvroEvaluatorType.");
    }
  }

  public static AvroEvaluatorProcess toAvroEvaluatorProcess(final EvaluatorProcess evaluatorProcess) {
    final List<CharSequence> commandLines = new ArrayList<>();
    for (final  String commandLine : evaluatorProcess.getCommandLine()) {
      commandLines.add(commandLine);
    }

    return AvroEvaluatorProcess.newBuilder()
        .setCommandLines(commandLines)
        .setEvaluatorType(toAvroEvaluatorType(evaluatorProcess.getType()))
        .setIsOptionSet(evaluatorProcess.isOptionSet())
        .build();
  }

  public static AvroEvaluatorDescriptor toAvroEvaluatorDescriptor(final EvaluatorDescriptor evaluatorDescriptor) {
    return AvroEvaluatorDescriptor.newBuilder()
        .setMemory(evaluatorDescriptor.getMemory())
        .setNodeDescriptor(toAvroNodeDescriptor(evaluatorDescriptor.getNodeDescriptor()))
        .setNumberOfCores(evaluatorDescriptor.getNumberOfCores())
        .setProcess(toAvroEvaluatorProcess(evaluatorDescriptor.getProcess()))
        .build();
  }

  public static AvroRuntimeStart toAvroRuntimeStart(final RuntimeStart runtimeStart) {
    return AvroRuntimeStart.newBuilder()
        .setTimestamp(runtimeStart.getTimestamp())
        .build();
  }

  public static AvroStartTime toAvroStartTime(final StartTime startTime) {
    return AvroStartTime.newBuilder()
        .setTimestamp(startTime.getTimestamp())
        .build();
  }

  public static AvroStopTime toAvroStopTime(final StopTime stopTime) {
    return AvroStopTime.newBuilder()
        .setTimestamp(stopTime.getTimestamp())
        .build();
  }

  public static AvroRuntimeStop toAvroRuntimeStop(final RuntimeStop runtimeStop) {
    return AvroRuntimeStop.newBuilder()
        .setException(convertThrowableToString(runtimeStop.getException()))
        .setTimestamp(runtimeStop.getTimestamp())
        .build();
  }

  public static AvroContextBase toAvroContextBase(final ContextBase contextBase) {
    return AvroContextBase.newBuilder()
        .setEvaluatorDescriptor(null)
        .setEvaluatorId(contextBase.getEvaluatorId())
        .setId(contextBase.getId())
        .setParentId(contextBase.getParentId().orElse(null))
        .build();
  }

  public static AvroActiveContext toAvroActiveContext(final ActiveContext activeContext) {
    return AvroActiveContext.newBuilder()
        .setBase(toAvroContextBase(activeContext))
        .build();
  }

  public static AvroClosedContext toAvroClosedContext(final ClosedContext closedContext) {
    return AvroClosedContext.newBuilder()
        .setBase(toAvroContextBase(closedContext))
        .setParentContext(toAvroActiveContext(closedContext.getParentContext()))
        .build();
  }

  public static AvroFailedContext toAvroFailedContext(final FailedContext failedContext) {
    return AvroFailedContext.newBuilder()
        .setBase(toAvroContextBase(failedContext))
        .setParentContext(unwrapOptionalActiveContext(failedContext.getParentContext()))
        .setFailure(toAvroFailure(failedContext))
        .build();
  }

  public static AvroCompletedTask toAvroCompletedTask(final CompletedTask completedTask) {
    return AvroCompletedTask.newBuilder()
        .setId(completedTask.getId())
        .setActiveContext(toAvroActiveContext(completedTask.getActiveContext()))
        .setGet(wrapNullableByteArray(completedTask.get()))
        .build();
  }

  public static AvroFailedTask toAvroFailedTask(final FailedTask failedTask) {
    return AvroFailedTask.newBuilder()
        .setActiveContext(unwrapOptionalActiveContext(failedTask.getActiveContext()))
        .setFailure(toAvroFailure(failedTask))
        .build();
  }

  public static AvroRunningTask toAvroRunningTask(final RunningTask runningTask) {
    return AvroRunningTask.newBuilder()
        .setActiveContext(toAvroActiveContext(runningTask.getActiveContext()))
        .setId(runningTask.getId())
        .build();
  }

  public static AvroTaskMessage toAvroTaskMessage(final TaskMessage taskMessage) {
    return AvroTaskMessage.newBuilder()
        .setId(taskMessage.getId())
        .setContextId(taskMessage.getContextId())
        .setMessageSourceId(taskMessage.getMessageSourceID())
        .setGet(wrapNullableByteArray(taskMessage.get()))
        .build();
  }

  public static AvroSuspendedTask toAvroSuspendedTask(final SuspendedTask suspendedTask) {
    return AvroSuspendedTask.newBuilder()
        .setGet(wrapNullableByteArray(suspendedTask.get()))
        .setId(suspendedTask.getId())
        .setActiveContext(toAvroActiveContext(suspendedTask.getActiveContext()))
        .build();
  }

  public static AvroAllocatedEvaluator toAvroAllocatedEvaluator(final AllocatedEvaluator allocatedEvaluator) {
    return AvroAllocatedEvaluator.newBuilder()
        .setId(allocatedEvaluator.getId())
        .setEvaluatorDescriptor(toAvroEvaluatorDescriptor(allocatedEvaluator.getEvaluatorDescriptor()))
        .build();
  }

  public static AvroFailedEvaluator toAvroFailedEvaluator(final FailedEvaluator failedEvaluator) {
    final AvroFailedTask avroFailedTask;
    if (failedEvaluator.getFailedTask().isPresent()) {
      avroFailedTask = toAvroFailedTask(failedEvaluator.getFailedTask().get());
    } else {
      avroFailedTask = null;
    }

    final List<AvroFailedContext> avroFailedContextList = new ArrayList<>();
    for (final FailedContext failedContext : failedEvaluator.getFailedContextList()) {
      avroFailedContextList.add(toAvroFailedContext(failedContext));
    }

    return AvroFailedEvaluator.newBuilder()
        .setId(failedEvaluator.getId())
        .setEvaluatorException(convertThrowableToString(failedEvaluator.getEvaluatorException()))
        .setFailedContextList(avroFailedContextList)
        .setFailedTask(avroFailedTask)
        .build();
  }

  public static AvroCompletedEvaluator toAvroCompletedEvaluator(final CompletedEvaluator completedEvaluator) {
    return AvroCompletedEvaluator.newBuilder()
        .setId(completedEvaluator.getId())
        .build();
  }

  public static String toString(final SpecificRecord record) {
    final String jsonEncodedRecord;
    try {
      final Schema schema = record.getSchema();
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      final Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bos);
      final SpecificDatumWriter datumWriter = new SpecificDatumWriter(record.getClass());
      datumWriter.write(record, encoder);
      encoder.flush();
      jsonEncodedRecord = new String(bos.toByteArray(), Charset.forName("UTF-8"));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return jsonEncodedRecord;
  }

  private static AvroActiveContext unwrapOptionalActiveContext(final Optional<ActiveContext> optionalActiveContext) {
    if (optionalActiveContext.isPresent()) {
      return toAvroActiveContext(optionalActiveContext.get());
    }

    return null;
  }

  private static String convertThrowableToString(final Throwable throwable) {
    if (throwable != null) {
      return throwable.toString();
    }

    return null;
  }

  private static ByteBuffer wrapNullableByteArray(final byte[] data) {
    if (data != null) {
      return ByteBuffer.wrap(data);
    }

    return null;
  }

  private static ByteBuffer unwrapOptionalByteArray(final Optional<byte[]> optionalByteArray) {
    if (optionalByteArray.isPresent()) {
      return ByteBuffer.wrap(optionalByteArray.get());
    }

    return null;
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private WatcherAvroUtil() {
  }
}
