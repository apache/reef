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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Test Avro Http Serializer
 */
public final class TestAvroHttpSerializer {
  private AvroHttpRequest avroRequest;
  private AvroHttpSerializer avroHttpSerializer;

  @Before
  public void setUp() throws Exception {
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(Charset.forName("UTF-8"));
    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString("id=12&id=34&a=b")
        .setPathInfo("/reef/evaluators")
        .setInputStream(ByteBuffer.wrap(b))
        .build();
    avroHttpSerializer = new AvroHttpSerializer();
  }

  @Test
  public void testJSonsStringRoundTrip() {
    final String serializedString = avroHttpSerializer.toString(avroRequest);
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromString(serializedString);
    assertEqual(avroRequest, deserializedRequest);
  }

  @Test
  public void testBytesRoundTrip() {
    final byte[] serializedBytes = avroHttpSerializer.toBytes(avroRequest);
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromBytes(serializedBytes);
    assertEqual(avroRequest, deserializedRequest);
  }

  private void assertEqual(final AvroHttpRequest request1, final AvroHttpRequest request2) {
    Assert.assertEquals(request1.getHttpMethod(), request2.getHttpMethod().toString());
    Assert.assertEquals(request1.getQueryString(), request2.getQueryString().toString());
    Assert.assertEquals(request1.getPathInfo(), request2.getPathInfo().toString());
    Assert.assertEquals(request1.getRequestUrl(), request2.getRequestUrl().toString());
    Assert.assertEquals(request1.getInputStream(), request2.getInputStream());
  }
}
