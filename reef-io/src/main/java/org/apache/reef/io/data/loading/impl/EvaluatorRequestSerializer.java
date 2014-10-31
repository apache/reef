/**
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

/**
 * Serialize and deserialize EvaluatorRequest objects
 * Currently only supports number & memory
 * Does not take care of Resource Descriptor
 */
public class EvaluatorRequestSerializer {
  public static String serialize(EvaluatorRequest request) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream daos = new DataOutputStream(baos)) {

        daos.writeInt(request.getNumber());
        daos.writeInt(request.getMegaBytes());
        daos.writeInt(request.getNumberOfCores());

      } catch (IOException e) {
        throw e;
      }

      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e1) {
      throw new RuntimeException("Unable to serialize compute request", e1);
    }
  }

  public static EvaluatorRequest deserialize(String serializedRequest) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serializedRequest))) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        return EvaluatorRequest.newBuilder()
            .setNumber(dais.readInt())
            .setMemory(dais.readInt())
            .setNumberOfCores(dais.readInt())
            .build();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to de-serialize compute request", e);
    }
  }
}
