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

package org.apache.reef.runtime.multi.utils;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.runtime.multi.utils.avro.MultiRuntimeDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializer for MultiRuntimeDefinition.
 */
public final class MultiRuntimeDefinitionSerializer {

  private static final String CHARSET_NAME = "UTF-8";

  /**
   * Serializes MultiRuntimeDefinition.
   * @param runtimeDefinition the Avro object to serialize
   * @return Serialized avro string
   */
  public String serialize(final MultiRuntimeDefinition runtimeDefinition){
    final DatumWriter<MultiRuntimeDefinition> configurationWriter =
            new SpecificDatumWriter<>(MultiRuntimeDefinition.class);
    final String serializedConfiguration;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(runtimeDefinition.getSchema(), out);
      configurationWriter.write(runtimeDefinition, encoder);
      encoder.flush();
      out.flush();
      serializedConfiguration = out.toString(CHARSET_NAME);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return serializedConfiguration;
  }

  /**
   * Deserializes avro definition.
   * @param serializedRuntimeDefinition serialized definition
   * @return Avro object
   * @throws IOException
   */
  public MultiRuntimeDefinition deserialize(final String serializedRuntimeDefinition) throws
          IOException{
    final JsonDecoder decoder = DecoderFactory.get().
            jsonDecoder(MultiRuntimeDefinition.getClassSchema(), serializedRuntimeDefinition);
    final SpecificDatumReader<MultiRuntimeDefinition> reader = new SpecificDatumReader<>(MultiRuntimeDefinition.class);
    MultiRuntimeDefinition rd = reader.read(null, decoder);
    return rd;
  }
}
