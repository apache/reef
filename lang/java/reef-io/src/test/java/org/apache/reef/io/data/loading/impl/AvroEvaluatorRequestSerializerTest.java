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

import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test AvroEvaluatorRequestSerializer.
 */
public class AvroEvaluatorRequestSerializerTest {
  @Test
  public void testSerializeDeserializeCompleteRequest() {
    final EvaluatorRequest originalRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(10)
        .setNumberOfCores(3)
        .addRackName("rack1")
        .addRackName("rack2")
        .addNodeName("nodename1")
        .addNodeName("nodename2")
        .build();
    final String serializedRequest = AvroEvaluatorRequestSerializer.toString(originalRequest);
    final EvaluatorRequest deserializedRequest = AvroEvaluatorRequestSerializer.fromString(serializedRequest);

    Assert.assertEquals(originalRequest.getMegaBytes(), deserializedRequest.getMegaBytes());
    Assert.assertEquals(originalRequest.getNumber(), deserializedRequest.getNumber());
    Assert.assertEquals(originalRequest.getNumberOfCores(), deserializedRequest.getNumberOfCores());
    Assert.assertEquals(originalRequest.getRackNames(), deserializedRequest.getRackNames());
    Assert.assertEquals(originalRequest.getNodeNames(), deserializedRequest.getNodeNames());
  }

  @Test
  public void testSerializeDeserializeNoRacksNorNodesRequest() {
    final EvaluatorRequest originalRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(10)
        .setNumberOfCores(3)
        .build();
    final String serialized = AvroEvaluatorRequestSerializer.toString(originalRequest);
    final EvaluatorRequest deserializedRequest = AvroEvaluatorRequestSerializer.fromString(serialized);

    Assert.assertEquals(originalRequest.getMegaBytes(), deserializedRequest.getMegaBytes());
    Assert.assertEquals(originalRequest.getNumber(), deserializedRequest.getNumber());
    Assert.assertEquals(originalRequest.getNumberOfCores(), deserializedRequest.getNumberOfCores());
    Assert.assertTrue(deserializedRequest.getRackNames().size() == 0);
    Assert.assertTrue(deserializedRequest.getNodeNames().size() == 0);
  }
}
