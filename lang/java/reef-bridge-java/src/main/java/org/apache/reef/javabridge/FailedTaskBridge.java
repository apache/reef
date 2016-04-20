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
package org.apache.reef.javabridge;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.javabridge.avro.AvroFailedTask;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.task.FailedTask}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "FailedTaskClr2Java.cpp" },
    CsFiles = { "IFailedTaskClr2Java.cs", "FailedTask.cs" })
public final class FailedTaskBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(FailedTaskBridge.class.getName());

  private final FailedTask jfailedTask;
  private final ActiveContextBridge jactiveContext;
  private final byte[] failedTaskSerializedAvro;

  public FailedTaskBridge(final FailedTask failedTask, final ActiveContextBridgeFactory factory) {
    this.jfailedTask = failedTask;
    if (failedTask.getActiveContext().isPresent()) {
      this.jactiveContext = factory.getActiveContextBridge(failedTask.getActiveContext().get());
    } else {
      this.jactiveContext = null;
    }

    try {
      this.failedTaskSerializedAvro = generateFailedTaskSerializedAvro();
    } catch(final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ActiveContextBridge getActiveContext() {
    return jactiveContext;
  }

  public byte[] getFailedTaskSerializedAvro() {
    return failedTaskSerializedAvro;
  }

  private byte[] generateFailedTaskSerializedAvro() throws IOException {
    AvroFailedTask avroFailedTask = null;

    if (jfailedTask.getData() != null && jfailedTask.getData().isPresent()) {
      // Deserialize what was passed in from C#.
      try (final ByteArrayInputStream fileInputStream = new ByteArrayInputStream(jfailedTask.getData().get())) {
        final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
            AvroFailedTask.getClassSchema(), fileInputStream);
        final SpecificDatumReader<AvroFailedTask> reader =
            new SpecificDatumReader<>(AvroFailedTask.class);
        avroFailedTask = reader.read(null, decoder);
      }
    } else {
      // This may result from a failed Evaluator.
      avroFailedTask = AvroFailedTask.newBuilder()
          .setIdentifier(jfailedTask.getId())
          .setCause(ByteBuffer.wrap(new byte[0]))
          .setData(ByteBuffer.wrap(new byte[0]))
          .setMessage("")
          .build();
    }

    // Overwrite the message if Java provides a message and C# does not.
    // Typically the case for failed Evaluators.
    if (StringUtils.isNoneBlank(jfailedTask.getMessage()) &&
        StringUtils.isBlank(avroFailedTask.getMessage().toString())) {
      avroFailedTask.setMessage(jfailedTask.getMessage());
    }

    final DatumWriter<AvroFailedTask> datumWriter = new SpecificDatumWriter<>(AvroFailedTask.class);

    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroFailedTask.getSchema(), outputStream);
      datumWriter.write(avroFailedTask, encoder);
      encoder.flush();
      outputStream.flush();
      return outputStream.toByteArray();
    }
  }

  @Override
  public void close() {
  }
}

