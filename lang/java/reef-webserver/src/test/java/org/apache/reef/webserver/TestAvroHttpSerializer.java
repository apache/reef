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

import org.apache.avro.AvroRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.ServletException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Test Avro Http Serializer.
 */
public final class TestAvroHttpSerializer {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private AvroHttpRequest avroRequest;
  private AvroHttpSerializer avroHttpSerializer;

  public static String readStream(final InputStream is) {
    final StringBuilder sb = new StringBuilder(512);
    try {
      final Reader r = new InputStreamReader(is, StandardCharsets.UTF_8);
      int c = 0;
      while ((c = r.read()) != -1) {
        sb.append((char) c);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  @Before
  public void setUp() throws Exception {
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(StandardCharsets.UTF_8);
    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString("id=12&id=34&a=b")
        .setPathInfo("/reef/evaluators")
        .setInputStream(ByteBuffer.wrap(b))
        .setHeader(createHeader())
        .build();
    avroHttpSerializer = new AvroHttpSerializer();
  }

  /**
   * Test serialize to file then deserialize from file.
   * @throws IOException
   * @throws ServletException
   */
  @Test
  public void testFileRoundTrip() throws IOException, ServletException {
    final File f = new File("httpRequestData.bin");
    avroHttpSerializer.toFile(avroRequest, f);
    final AvroHttpRequest avroRequest2 = avroHttpSerializer.fromFile(f);
    f.delete();
    assertEqual(avroRequest, avroRequest2);
  }


  /**
   * Test serialize to bytes, then write to file, then read from file, finally deserialize.
   * @throws IOException
   * @throws ServletException
   */
  @Test
  public void testFile1RoundTrip() throws IOException, ServletException {
    final byte[] bytes = avroHttpSerializer.toBytes(avroRequest);
    final File f = new File("httpRequestData.bin");
    final OutputStream os = new FileOutputStream(f);
    os.write(bytes, 0, bytes.length);
    os.close();

    //final File f = new File("httpRequestData.bin");
    final byte[] bytes1 = new byte[(int) f.length()];
    final InputStream is = new FileInputStream("httpRequestData.bin");
    is.read(bytes1, 0, bytes1.length);

    is.close();
    f.delete();
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromBytes(bytes1);

    assertEqual(avroRequest, deserializedRequest);
  }

  /**
   * Test serialize to string, then write to file, then read from file, finally deserialize.
   * @throws IOException
   * @throws ServletException
   */
  @Test
  public void testFile2RoundTrip() throws IOException, ServletException {
    final String serializedString = avroHttpSerializer.toString(avroRequest);

    final File f = new File("httpRequestData.txt");
    final OutputStream os = new FileOutputStream(f); //"httpRequestData.txt");
    final OutputStreamWriter sw = new OutputStreamWriter(os);
    sw.write(serializedString);
    sw.flush();
    sw.close();
    os.close();

    final InputStream is = new FileInputStream(f);
    final String all = readStream(is);
    is.close();
    f.delete();

    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromString(all);

    assertEqual(avroRequest, deserializedRequest);
  }

  /**
   * Test serialize to JSon string, the deserialize from it.
   */
  @Test
  public void testJSonsStringRoundTrip() {
    final String serializedString = avroHttpSerializer.toString(avroRequest);
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromString(serializedString);
    assertEqual(avroRequest, deserializedRequest);
  }

  /**
   * Test serialize to bytes, the deserialize from it.
   */
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

  /**
   * Test null incomplete request.
   */
  @Test
  public void testIncompleteData() {
    thrown.expect(AvroRuntimeException.class);
    thrown.expectMessage("Field queryString type:STRING pos:3 not set and has no default value");
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(StandardCharsets.UTF_8);
    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setPathInfo("/reef/evaluators")
        .setInputStream(ByteBuffer.wrap(b))
        .setHeader(createHeader())
        .build();
  }

  /**
   * Test null query string.
   */
  @Test
  public void testNullData() {
    thrown.expect(AvroRuntimeException.class);
    thrown.expectMessage("Field queryString type:STRING pos:3 does not accept null values");
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(StandardCharsets.UTF_8);
    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString(null)
        .setPathInfo("/reef/evaluators")
        .setHeader(createHeader())
        .setInputStream(ByteBuffer.wrap(b))
        .build();
  }

  /**
   * Test null bytes.
   */
  @Test
  public void testNullBytes() {
    thrown.expect(AvroRuntimeException.class);
    thrown.expectMessage("Field inputStream type:BYTES pos:5 does not accept null values");
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(StandardCharsets.UTF_8);

    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString("id=12&id=34&a=b")
        .setPathInfo("/reef/evaluators")
        .setInputStream(null)
        .setHeader(createHeader())
        .build();
  }

  /**
   * Test empty string.
   */
  @Test
  public void testEmptyString() {
    final String s = "test binary stream data";
    final byte[] b = s.getBytes(StandardCharsets.UTF_8);

    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString("")
        .setPathInfo("/reef/evaluators")
        .setInputStream(ByteBuffer.wrap(b))
        .setHeader(createHeader())
        .build();

    avroHttpSerializer = new AvroHttpSerializer();
    final byte[] serializedBytes = avroHttpSerializer.toBytes(avroRequest);
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromBytes(serializedBytes);
    assertEqual(avroRequest, deserializedRequest);
  }

  /**
   * Test empty bytes.
   */
  @Test
  public void testEmptyBytes() {
    final byte[] b = new byte[0];

    avroRequest = AvroHttpRequest.newBuilder()
        .setRequestUrl("http://localhost:8080/reef/evaluators?id=12&id=34&a=b")
        .setHttpMethod("POST")
        .setQueryString("")
        .setPathInfo("/reef/evaluators")
        .setInputStream(ByteBuffer.wrap(b))
        .setHeader(createHeader())
        .build();
    avroHttpSerializer = new AvroHttpSerializer();
    final byte[] serializedBytes = avroHttpSerializer.toBytes(avroRequest);
    final AvroHttpRequest deserializedRequest = avroHttpSerializer.fromBytes(serializedBytes);
    assertEqual(avroRequest, deserializedRequest);
  }

  private ArrayList<HeaderEntry> createHeader() {
    final ArrayList<HeaderEntry> list = new ArrayList<>();
    final HeaderEntry e1 = HeaderEntry.newBuilder()
        .setKey("a")
        .setValue("xxx")
        .build();
    list.add(e1);

    final HeaderEntry e2 = HeaderEntry.newBuilder()
        .setKey("b")
        .setValue("yyy")
        .build();
    list.add(e2);

    return list;
  }
}
