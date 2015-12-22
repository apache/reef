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
package org.apache.reef.vortex.common;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.common.avro.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialize and deserialize Vortex message to/from byte array.
 */
@Private
@DriverSide
@Unstable
public final class VortexAvroUtils {
  /**
   * Serialize VortexRequest to byte array.
   * @param vortexRequest Vortex request message to serialize.
   * @return Serialized byte array.
   */
  public static byte[] toBytes(final VortexRequest vortexRequest) {
    // Convert VortexRequest message to Avro message.
    final AvroVortexRequest avroVortexRequest;
    switch (vortexRequest.getType()) {
    case ExecuteTasklet:
      final TaskletExecutionRequest taskletExecutionRequest = (TaskletExecutionRequest) vortexRequest;
      // The following TODOs are sub-issues of cleaning up Serializable in Vortex (REEF-504).
      // The purpose is to reduce serialization cost, which leads to bottleneck in Master.
      // Temporarily those are left as TODOs, but will be addressed in separate PRs.
      final VortexFunction vortexFunction = taskletExecutionRequest.getFunction();
      final byte[] serializedInput = vortexFunction.getInputCodec().encode(taskletExecutionRequest.getInput());
      // TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction
      final byte[] serializedFunction = SerializationUtils.serialize(vortexFunction);
      avroVortexRequest = AvroVortexRequest.newBuilder()
          .setRequestType(AvroRequestType.ExecuteTasklet)
          .setTaskletRequest(
              AvroTaskletExecutionRequest.newBuilder()
                  .setTaskletId(taskletExecutionRequest.getTaskletId())
                  .setSerializedInput(ByteBuffer.wrap(serializedInput))
                  .setSerializedUserFunction(ByteBuffer.wrap(serializedFunction))
                  .build())
          .build();
      break;
    case CancelTasklet:
      final TaskletCancellationRequest taskletCancellationRequest = (TaskletCancellationRequest) vortexRequest;
      avroVortexRequest = AvroVortexRequest.newBuilder()
          .setRequestType(AvroRequestType.CancelTasklet)
          .setTaskletRequest(
              AvroTaskletCancellationRequest.newBuilder()
                  .setTaskletId(taskletCancellationRequest.getTaskletId())
                  .build())
          .build();
      break;
    default:
      throw new RuntimeException("Undefined message type");
    }

    // Serialize the Avro message to byte array.
    return toBytes(avroVortexRequest, AvroVortexRequest.class);
  }

  /**
   * Serialize WorkerReport to byte array.
   * @param workerReport Worker report message to serialize.
   * @return Serialized byte array.
   */
  public static byte[] toBytes(final WorkerReport workerReport) {
    final List<AvroTaskletReport> workerTaskletReports = new ArrayList<>();

    for (final TaskletReport taskletReport : workerReport.getTaskletReports()) {
      final AvroTaskletReport avroTaskletReport;
      switch (taskletReport.getType()) {
      case TaskletResult:
        final TaskletResultReport taskletResultReport = (TaskletResultReport) taskletReport;
        avroTaskletReport = AvroTaskletReport.newBuilder()
            .setReportType(AvroReportType.TaskletResult)
            .setTaskletReport(
                AvroTaskletResultReport.newBuilder()
                    .setTaskletId(taskletResultReport.getTaskletId())
                    .setSerializedOutput(ByteBuffer.wrap(taskletResultReport.getSerializedResult()))
                    .build())
            .build();
        break;
      case TaskletAggregationResult:
        final TaskletAggregationResultReport taskletAggregationResultReport =
            (TaskletAggregationResultReport) taskletReport;
        avroTaskletReport = AvroTaskletReport.newBuilder()
            .setReportType(AvroReportType.TaskletAggregationResult)
            .setTaskletReport(
                AvroTaskletAggregationResultReport.newBuilder()
                    .setTaskletIds(taskletAggregationResultReport.getTaskletIds())
                    .setSerializedOutput(ByteBuffer.wrap(taskletAggregationResultReport.getSerializedResult()))
                    .build())
            .build();
        break;
      case TaskletCancelled:
        final TaskletCancelledReport taskletCancelledReport = (TaskletCancelledReport) taskletReport;
        avroTaskletReport = AvroTaskletReport.newBuilder()
            .setReportType(AvroReportType.TaskletCancelled)
            .setTaskletReport(
                AvroTaskletCancelledReport.newBuilder()
                    .setTaskletId(taskletCancelledReport.getTaskletId())
                    .build())
            .build();
        break;
      case TaskletFailure:
        final TaskletFailureReport taskletFailureReport = (TaskletFailureReport) taskletReport;
        final byte[] serializedException = SerializationUtils.serialize(taskletFailureReport.getException());
        avroTaskletReport = AvroTaskletReport.newBuilder()
            .setReportType(AvroReportType.TaskletFailure)
            .setTaskletReport(
                AvroTaskletFailureReport.newBuilder()
                    .setTaskletId(taskletFailureReport.getTaskletId())
                    .setSerializedException(ByteBuffer.wrap(serializedException))
                    .build())
            .build();
        break;
      case TaskletAggregationFailure:
        final TaskletAggregationFailureReport taskletAggregationFailureReport =
            (TaskletAggregationFailureReport) taskletReport;
        final byte[] serializedAggregationException =
            SerializationUtils.serialize(taskletAggregationFailureReport.getException());
        avroTaskletReport = AvroTaskletReport.newBuilder()
            .setReportType(AvroReportType.TaskletAggregationFailure)
            .setTaskletReport(
                AvroTaskletAggregationFailureReport.newBuilder()
                    .setTaskletIds(taskletAggregationFailureReport.getTaskletIds())
                    .setSerializedException(ByteBuffer.wrap(serializedAggregationException))
                    .build())
            .build();
        break;
      default:
        throw new RuntimeException("Undefined message type");
      }

      workerTaskletReports.add(avroTaskletReport);
    }

    // Convert WorkerReport message to Avro message.
    final AvroWorkerReport avroWorkerReport = AvroWorkerReport.newBuilder()
        .setTaskletReports(workerTaskletReports)
        .build();

    // Serialize the Avro message to byte array.
    return toBytes(avroWorkerReport, AvroWorkerReport.class);
  }

  /**
   * Deserialize byte array to VortexRequest.
   * @param bytes Byte array to deserialize.
   * @return De-serialized VortexRequest.
   */
  public static VortexRequest toVortexRequest(final byte[] bytes) {
    final AvroVortexRequest avroVortexRequest = toAvroObject(bytes, AvroVortexRequest.class);

    final VortexRequest vortexRequest;
    switch (avroVortexRequest.getRequestType()) {
    case ExecuteTasklet:
      final AvroTaskletExecutionRequest taskletExecutionRequest =
          (AvroTaskletExecutionRequest)avroVortexRequest.getTaskletRequest();
      // TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction
      final VortexFunction function =
          (VortexFunction) SerializationUtils.deserialize(
              taskletExecutionRequest.getSerializedUserFunction().array());
      vortexRequest = new TaskletExecutionRequest(taskletExecutionRequest.getTaskletId(), function,
         function.getInputCodec().decode(taskletExecutionRequest.getSerializedInput().array()));
      break;
    case CancelTasklet:
      final AvroTaskletCancellationRequest taskletCancellationRequest =
          (AvroTaskletCancellationRequest)avroVortexRequest.getTaskletRequest();
      vortexRequest = new TaskletCancellationRequest(taskletCancellationRequest.getTaskletId());
      break;
    default:
      throw new RuntimeException("Undefined VortexRequest type");
    }
    return vortexRequest;
  }

  /**
   * Deserialize byte array to WorkerReport.
   * @param bytes Byte array to deserialize.
   * @return De-serialized WorkerReport.
   */
  public static WorkerReport toWorkerReport(final byte[] bytes) {
    final AvroWorkerReport avroWorkerReport = toAvroObject(bytes, AvroWorkerReport.class);
    final List<TaskletReport> workerTaskletReports = new ArrayList<>();

    for (final AvroTaskletReport avroTaskletReport : avroWorkerReport.getTaskletReports()) {
      final TaskletReport taskletReport;

      switch (avroTaskletReport.getReportType()) {
      case TaskletResult:
        final AvroTaskletResultReport taskletResultReport =
            (AvroTaskletResultReport)avroTaskletReport.getTaskletReport();
        taskletReport = new TaskletResultReport(taskletResultReport.getTaskletId(),
            taskletResultReport.getSerializedOutput().array());
        break;
      case TaskletAggregationResult:
        final AvroTaskletAggregationResultReport taskletAggregationResultReport =
            (AvroTaskletAggregationResultReport)avroTaskletReport.getTaskletReport();
        taskletReport =
            new TaskletAggregationResultReport(taskletAggregationResultReport.getTaskletIds(),
                taskletAggregationResultReport.getSerializedOutput().array());
        break;
      case TaskletCancelled:
        final AvroTaskletCancelledReport taskletCancelledReport =
            (AvroTaskletCancelledReport)avroTaskletReport.getTaskletReport();
        taskletReport = new TaskletCancelledReport(taskletCancelledReport.getTaskletId());
        break;
      case TaskletFailure:
        final AvroTaskletFailureReport taskletFailureReport =
            (AvroTaskletFailureReport)avroTaskletReport.getTaskletReport();
        final Exception exception =
            (Exception) SerializationUtils.deserialize(taskletFailureReport.getSerializedException().array());
        taskletReport = new TaskletFailureReport(taskletFailureReport.getTaskletId(), exception);
        break;
      case TaskletAggregationFailure:
        final AvroTaskletAggregationFailureReport taskletAggregationFailureReport =
            (AvroTaskletAggregationFailureReport)avroTaskletReport.getTaskletReport();
        final Exception aggregationException =
            (Exception) SerializationUtils.deserialize(
                taskletAggregationFailureReport.getSerializedException().array());
        taskletReport =
            new TaskletAggregationFailureReport(taskletAggregationFailureReport.getTaskletIds(), aggregationException);
        break;
      default:
        throw new RuntimeException("Undefined TaskletReport type");
      }

      workerTaskletReports.add(taskletReport);
    }

    return new WorkerReport(workerTaskletReports);
  }

  /**
   * Serialize Avro object to byte array.
   * @param avroObject Avro object to serialize.
   * @param theClass Class of the Avro object.
   * @param <T> Type of the Avro object.
   * @return Serialized byte array.
   */
  private static <T> byte[] toBytes(final T avroObject, final Class<T> theClass) {
    final DatumWriter<T> reportWriter = new SpecificDatumWriter<>(theClass);
    final byte[] theBytes;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      reportWriter.write(avroObject, encoder);
      encoder.flush();
      out.flush();
      theBytes = out.toByteArray();
      return theBytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize byte array to Avro object.
   * @param bytes Byte array to deserialize.
   * @param theClass Class of the Avro object.
   * @param <T> Type of the Avro object.
   * @return Avro object de-serialized from byte array.
   */
  private static <T> T toAvroObject(final byte[] bytes, final Class<T> theClass) {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    final SpecificDatumReader<T> reader = new SpecificDatumReader<>(theClass);
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private VortexAvroUtils() {
  }
}
