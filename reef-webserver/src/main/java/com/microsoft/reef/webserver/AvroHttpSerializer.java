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

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serialize Http Request Response data with Avro
 */
public class AvroHttpSerializer {
  /**
   * constant definition of Json charset
   */
  public static final String JSON_CHARSET = "ISO-8859-1";

  public AvroHttpSerializer() {
  }

  /**
   * Convert from HttpServletRequest to AvroHttpRequest
   *
   * @param request
   * @return
   * @throws ServletException
   * @throws IOException
   */
  public AvroHttpRequest toAvro(final HttpServletRequest request) throws ServletException, IOException {
    final ParsedHttpRequest requestParser = new ParsedHttpRequest(request);
    return AvroHttpRequest.newBuilder()
        .setRequestUrl(requestParser.getRequestUrl())
        .setHttpMethod(requestParser.getMethod())
        .setQueryString(requestParser.getQueryString())
        .setPathInfo(requestParser.getPathInfo())
        .setInputStream(ByteBuffer.wrap(requestParser.getInputStream()))
        .build();
  }

  /**
   * From AvroHttpRequest to Jason String
   *
   * @param request
   * @return
   */
  public String toString(final AvroHttpRequest request) {
    final DatumWriter<AvroHttpRequest> configurationWriter1 = new SpecificDatumWriter<>(AvroHttpRequest.class);
    final String result;
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(AvroHttpRequest.SCHEMA$, out);
      configurationWriter1.write(request, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * From Json String to AvroHttpRequest
   *
   * @param jasonStr
   * @return
   */
  public AvroHttpRequest fromString(final String jasonStr) {
    final AvroHttpRequest request;
    try {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(AvroHttpRequest.getClassSchema(), jasonStr);
      final SpecificDatumReader<AvroHttpRequest> reader = new SpecificDatumReader<>(AvroHttpRequest.class);
      request = reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return request;
  }

  /**
   * From AvroHttpRequest to bytes
   *
   * @param request
   * @return
   */
  public byte[] toBytes(final AvroHttpRequest request) {
    final DatumWriter<AvroHttpRequest> requestWriter = new SpecificDatumWriter<>(AvroHttpRequest.class);
    final byte[] theBytes;
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      requestWriter.write(request, encoder);
      encoder.flush();
      out.flush();
      theBytes = out.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return theBytes;
  }

  /**
   * From bytes to AvroHttpRequest
   *
   * @param theBytes
   * @return
   */
  public AvroHttpRequest fromBytes(final byte[] theBytes) {
    final AvroHttpRequest request;
    try {
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
      final SpecificDatumReader<AvroHttpRequest> reader = new SpecificDatumReader<>(AvroHttpRequest.class);
      request = reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return request;
  }
}
