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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serialize Evaluator Info.
 * It is the default implementation for interface EvaluatorInfoSerializer.
 */
public class AvroEvaluatorInfoSerializer implements EvaluatorInfoSerializer {

  @Inject
  AvroEvaluatorInfoSerializer() {
  }

  /**
   * Create AvroEvaluatorsInfo object.
   */
  @Override
  public AvroEvaluatorsInfo toAvro(
      final List<String> ids, final Map<String, EvaluatorDescriptor> evaluators) {

    final List<AvroEvaluatorInfo> evaluatorsInfo = new ArrayList<>();

    for (final String id : ids) {

      final EvaluatorDescriptor evaluatorDescriptor = evaluators.get(id);
      String nodeId = null;
      String nodeName = null;
      InetSocketAddress address = null;
      int memory = 0;
      String type = null;
      String runtimeName = null;

      if (evaluatorDescriptor != null) {
        nodeId = evaluatorDescriptor.getNodeDescriptor().getId();
        nodeName = evaluatorDescriptor.getNodeDescriptor().getName();
        address = evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();
        memory = evaluatorDescriptor.getMemory();
        type = evaluatorDescriptor.getProcess().getType().toString();
        runtimeName = evaluatorDescriptor.getRuntimeName();
      }

      evaluatorsInfo.add(AvroEvaluatorInfo.newBuilder()
          .setEvaluatorId(id)
          .setNodeId(nodeId != null ? nodeId : "")
          .setNodeName(nodeName != null ? nodeName : "")
          .setInternetAddress(address != null ? address.toString() : "")
          .setMemory(memory)
          .setType(type != null ? type : "")
          .setRuntimeName(runtimeName != null ? runtimeName : "")
          .build());
    }

    return AvroEvaluatorsInfo.newBuilder()
        .setEvaluatorsInfo(evaluatorsInfo)
        .build();
  }

  /**
   * Convert AvroEvaluatorsInfo object to JSON string.
   */
  @Override
  public String toString(final AvroEvaluatorsInfo avroEvaluatorsInfo) {
    final DatumWriter<AvroEvaluatorsInfo> evaluatorWriter = new SpecificDatumWriter<>(AvroEvaluatorsInfo.class);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroEvaluatorsInfo.getSchema(), out);
      evaluatorWriter.write(avroEvaluatorsInfo, encoder);
      encoder.flush();
      return out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
