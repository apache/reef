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

import org.apache.commons.codec.binary.Base64;
import org.apache.reef.driver.evaluator.EvaluatorRequest;

import java.io.*;
import java.util.List;

/**
 * Serialize and deserialize EvaluatorRequest objects.
 * Supports number, memory, cores, nodeNames and rackNames serialization
 */
public final class EvaluatorRequestSerializer {
  public static String serialize(final EvaluatorRequest request) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {

        daos.writeInt(request.getNumber());
        daos.writeInt(request.getMegaBytes());
        daos.writeInt(request.getNumberOfCores());
        final List<String> nodeNames = request.getNodeNames();
        final List<String> rackNames = request.getRackNames();
        daos.writeInt(nodeNames.size());
        daos.writeInt(rackNames.size());
        for (final String nodeName : nodeNames) {
          daos.writeUTF(nodeName);
        }
        for (final String rackName : rackNames) {
          daos.writeUTF(rackName);
        }

      } catch (final IOException e) {
        throw e;
      }

      return Base64.encodeBase64String(baos.toByteArray());
    } catch (final IOException e1) {
      throw new RuntimeException("Unable to serialize compute request", e1);
    }
  }

  public static EvaluatorRequest deserialize(final String serializedRequest) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serializedRequest))) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final EvaluatorRequest.Builder builder = EvaluatorRequest.newBuilder()
            .setNumber(dais.readInt())
            .setMemory(dais.readInt())
            .setNumberOfCores(dais.readInt());
        final int numNodes = dais.readInt();
        final int numRacks = dais.readInt();
        for (int i = 0; i < numNodes; i++) {
          builder.addNodeName(dais.readUTF());
        }
        for (int i = 0; i < numRacks; i++) {
          builder.addRackName(dais.readUTF());
        }
        return builder.build();
      }
    } catch (final IOException e) {
      throw new RuntimeException("Unable to de-serialize compute request", e);
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private EvaluatorRequestSerializer() {
  }
}
