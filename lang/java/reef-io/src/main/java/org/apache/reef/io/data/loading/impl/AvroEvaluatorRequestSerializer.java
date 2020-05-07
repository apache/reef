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
package org.apache.reef.io.data.loading.impl;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.avro.AvroEvaluatorRequest;
import org.apache.reef.webserver.AvroHttpSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialize and deserialize EvaluatorRequest objects using Avro.
 */
@Private
public final class AvroEvaluatorRequestSerializer {
  private static AvroEvaluatorRequest toAvro(final EvaluatorRequest request) {
    final List<CharSequence> nodeNames = new ArrayList<>();
    for (final String nodeName : request.getNodeNames()) {
      nodeNames.add(nodeName);
    }

    final List<CharSequence> rackNames = new ArrayList<>();
    for (final String rackName : request.getRackNames()) {
      rackNames.add(rackName);
    }

    return AvroEvaluatorRequest.newBuilder()
        .setCores(request.getNumberOfCores())
        .setMegaBytes(request.getMegaBytes())
        .setNumber(request.getNumber())
        .setNodeNames(nodeNames)
        .setRackNames(rackNames)
        .build();
  }

  private static EvaluatorRequest fromAvro(final AvroEvaluatorRequest avroRequest) {
    final EvaluatorRequest.Builder builder = EvaluatorRequest.newBuilder()
        .setNumberOfCores(avroRequest.getCores())
        .setMemory(avroRequest.getMegaBytes())
        .setNumber(avroRequest.getNumber());
    for (final CharSequence nodeName : avroRequest.getNodeNames()) {
      builder.addNodeName(nodeName.toString());
    }
    for (final CharSequence rackName : avroRequest.getRackNames()) {
      builder.addRackName(rackName.toString());
    }
    return builder.build();
  }

  /**
   * Serialize EvaluatorRequest.
   */
  public static String toString(final EvaluatorRequest request) {
    AvroEvaluatorRequest avroRequest = toAvro(request);
    final DatumWriter<AvroEvaluatorRequest> datumWriter = new SpecificDatumWriter<>(AvroEvaluatorRequest.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroRequest.getSchema(), out);
      datumWriter.write(avroRequest, encoder);
      encoder.flush();
      out.close();
      return out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to serialize compute request", ex);
    }
  }

  /**
   * Deserialize EvaluatorRequest.
   */
  public static EvaluatorRequest fromString(final String serializedRequest) {
    try {
      final Decoder decoder =
          DecoderFactory.get().jsonDecoder(AvroEvaluatorRequest.getClassSchema(), serializedRequest);
      final SpecificDatumReader<AvroEvaluatorRequest> reader = new SpecificDatumReader<>(AvroEvaluatorRequest.class);
      return fromAvro(reader.read(null, decoder));
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to deserialize compute request", ex);
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private AvroEvaluatorRequestSerializer() {
  }
}
