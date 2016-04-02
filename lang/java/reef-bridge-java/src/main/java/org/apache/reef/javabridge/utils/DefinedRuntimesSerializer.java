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

package org.apache.reef.javabridge.utils;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.javabridge.avro.DefinedRuntimes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Serializer for MultiRuntimeDefinition.
 */
public final class DefinedRuntimesSerializer {

  private static final String CHARSET_NAME = "UTF-8";

  /**
   * Serializes DefinedRuntimes.
   * @param definedRuntimes the Avro object to toString
   * @return Serialized avro string
   */
  public byte[] toBytes(final DefinedRuntimes definedRuntimes){
    final DatumWriter<DefinedRuntimes> configurationWriter =
            new SpecificDatumWriter<>(DefinedRuntimes.class);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
      configurationWriter.write(definedRuntimes, binaryEncoder);
      binaryEncoder.flush();
      out.flush();
      return out.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to serialize DefinedRuntimes", e);
    }
  }

   /**
   * Deserializes avro definition.
   * @param serializedDefinedRuntimes serialized definition
   * @return Avro object
   * @throws IOException
   */
  public DefinedRuntimes fromBytes(final byte[] serializedDefinedRuntimes) throws
          IOException{
    try(InputStream is = new ByteArrayInputStream(serializedDefinedRuntimes)) {
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(is, null);
      final SpecificDatumReader<DefinedRuntimes> reader = new SpecificDatumReader<>(DefinedRuntimes.class);
      final DefinedRuntimes rd = reader.read(null, decoder);
      return rd;
    }
  }
}
