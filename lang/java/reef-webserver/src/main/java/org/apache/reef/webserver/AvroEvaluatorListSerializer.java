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
package org.apache.reef.webserver;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Serializer for Evaluator list.
 * It is the default implementation for interface EvaluatorListSerializer.
 */
public class AvroEvaluatorListSerializer implements EvaluatorListSerializer {

  @Inject
  AvroEvaluatorListSerializer() {
  }

  /**
   * Build AvroEvaluatorList object.
   */
  @Override
  public AvroEvaluatorList toAvro(
      final Map<String, EvaluatorDescriptor> evaluatorMap,
      final int totalEvaluators, final String startTime) {

    final List<AvroEvaluatorEntry> evaluatorEntities = new ArrayList<>();

    for (final Map.Entry<String, EvaluatorDescriptor> entry : evaluatorMap.entrySet()) {
      final EvaluatorDescriptor descriptor = entry.getValue();
      evaluatorEntities.add(AvroEvaluatorEntry.newBuilder()
              .setId(entry.getKey())
              .setName(descriptor.getNodeDescriptor().getName())
              .build());
    }

    return AvroEvaluatorList.newBuilder()
        .setEvaluators(evaluatorEntities)
        .setTotal(totalEvaluators)
        .setStartTime(startTime != null ? startTime : new Date().toString())
        .build();
  }

  /**
   * Convert AvroEvaluatorList to JSON string.
   */
  @Override
  public String toString(final AvroEvaluatorList avroEvaluatorList) {
    final DatumWriter<AvroEvaluatorList> evaluatorWriter = new SpecificDatumWriter<>(AvroEvaluatorList.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroEvaluatorList.getSchema(), out);
      evaluatorWriter.write(avroEvaluatorList, encoder);
      encoder.flush();
      return out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
