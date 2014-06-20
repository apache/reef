/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serialize Evaluator Info
 */
public class AvroEvaluatorInfoSerializer {
  public AvroEvaluatorsInfo toAvro(List<String> ids, Map<String, EvaluatorDescriptor> evaluators) {
    final List<AvroEvaluatorInfo> evaluatorsInfo = new ArrayList<>();

    for (String id : ids) {
      final EvaluatorDescriptor evaluatorDescriptor = evaluators.get(id);
      String nodeId = null;
      String nodeName = null;
      InetSocketAddress address = null;

      if (evaluatorDescriptor != null) {
        nodeId = evaluatorDescriptor.getNodeDescriptor().getId();
        nodeName = evaluatorDescriptor.getNodeDescriptor().getName();
        address = evaluatorDescriptor.getNodeDescriptor().getInetSocketAddress();
      }

      evaluatorsInfo.add(AvroEvaluatorInfo.newBuilder()
          .setEvaluatorId(id)
          .setNodeId(nodeId != null ? nodeId : "")
          .setNodeName(nodeName != null ? nodeName : "")
          .setInternetAddress(address != null ? address.toString() : "")
          .build());
    }

    return AvroEvaluatorsInfo.newBuilder()
        .setEvaluatorsInfo(evaluatorsInfo)
        .build();
  }

  public String toString(final AvroEvaluatorsInfo avroEvaluatorsInfo) {
    final DatumWriter<AvroEvaluatorsInfo> evaluatorWriter = new SpecificDatumWriter<>(AvroEvaluatorsInfo.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroEvaluatorsInfo.getSchema(), out);
      evaluatorWriter.write(avroEvaluatorsInfo, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}

