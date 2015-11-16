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
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.common.avro.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Serialize and deserialize Vortex message to/from byte array.
 */
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
      // TODO[REEF-1005]: Allow custom codecs for input/output data in Vortex.
      final byte[] serializedInput = SerializationUtils.serialize(taskletExecutionRequest.getInput());
      // TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction
      final byte[] serializedFunction = SerializationUtils.serialize(taskletExecutionRequest.getFunction());
      avroVortexRequest = AvroVortexRequest.newBuilder()
          .setRequestType(AvroRequestType.ExecuteTasklet)
          .setTaskletExecutionRequest(
              AvroTaskletExecutionRequest.newBuilder()
                  .setTaskletId(taskletExecutionRequest.getTaskletId())
                  .setSerializedInput(ByteBuffer.wrap(serializedInput))
                  .setSerializedUserFunction(ByteBuffer.wrap(serializedFunction))
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
    // Convert WorkerReport message to Avro message.
    final AvroWorkerReport avroWorkerReport;
    switch (workerReport.getType()) {
    case TaskletResult:
      final TaskletResultReport taskletResultReport = (TaskletResultReport) workerReport;
      // TODO[REEF-1005]: Allow custom codecs for input/output data in Vortex.
      final byte[] serializedOutput = SerializationUtils.serialize(taskletResultReport.getResult());
      avroWorkerReport = AvroWorkerReport.newBuilder()
          .setReportType(AvroReportType.TaskletResult)
          .setTaskletResult(
              AvroTaskletResultReport.newBuilder()
                  .setTaskletId(taskletResultReport.getTaskletId())
                  .setSerializedOutput(ByteBuffer.wrap(serializedOutput))
                  .build())
          .build();
      break;
    case TaskletFailure:
      final TaskletFailureReport taskletFailureReport = (TaskletFailureReport) workerReport;
      final byte[] serializedException = SerializationUtils.serialize(taskletFailureReport.getException());
      avroWorkerReport = AvroWorkerReport.newBuilder()
          .setReportType(AvroReportType.TaskletFailure)
          .setTaskletFailure(
              AvroTaskletFailureReport.newBuilder()
                  .setTaskletId(taskletFailureReport.getTaskletId())
                  .setSerializedException(ByteBuffer.wrap(serializedException))
                  .build())
          .build();
      break;
    default:
      throw new RuntimeException("Undefined message type");
    }

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
      final AvroTaskletExecutionRequest taskletExecutionRequest = avroVortexRequest.getTaskletExecutionRequest();
      // TODO[REEF-1003]: Use reflection instead of serialization when launching VortexFunction
      final VortexFunction function =
          (VortexFunction) SerializationUtils.deserialize(
              taskletExecutionRequest.getSerializedUserFunction().array());
      // TODO[REEF-1005]: Allow custom codecs for input/output data in Vortex.
      final Serializable input =
          (Serializable) SerializationUtils.deserialize(
              taskletExecutionRequest.getSerializedInput().array());
      vortexRequest = new TaskletExecutionRequest(taskletExecutionRequest.getTaskletId(), function, input);
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
    final WorkerReport workerReport;
    final AvroWorkerReport avroWorkerReport = toAvroObject(bytes, AvroWorkerReport.class);
    switch (avroWorkerReport.getReportType()) {
    case TaskletResult:
      final AvroTaskletResultReport taskletResultReport = avroWorkerReport.getTaskletResult();
      // TODO[REEF-1005]: Allow custom codecs for input/output data in Vortex.
      final Serializable output =
          (Serializable) SerializationUtils.deserialize(taskletResultReport.getSerializedOutput().array());
      workerReport = new TaskletResultReport<>(taskletResultReport.getTaskletId(), output);
      break;
    case TaskletFailure:
      final AvroTaskletFailureReport taskletFailureReport = avroWorkerReport.getTaskletFailure();
      final Exception exception =
          (Exception) SerializationUtils.deserialize(taskletFailureReport.getSerializedException().array());
      workerReport = new TaskletFailureReport(taskletFailureReport.getTaskletId(), exception);
      break;
    default:
      throw new RuntimeException("Undefined WorkerReport type");
    }
    return workerReport;
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
