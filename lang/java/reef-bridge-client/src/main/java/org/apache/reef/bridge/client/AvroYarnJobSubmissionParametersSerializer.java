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
package org.apache.reef.bridge.client;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.reef.bridge.client.avro.AvroYarnJobSubmissionParameters;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Searializer class for the AvroYarnAppSubmissionParameters.
 */
final class AvroYarnJobSubmissionParametersSerializer {
  @Inject
  private AvroYarnJobSubmissionParametersSerializer(){
  }

  /**
   * Reads avro object from file.
   *
   * @param file The file to read from
   * @return Avro object
   * @throws IOException
   */
  AvroYarnJobSubmissionParameters fromFile(final File file) throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      // This is mainly a test hook.
      return fromInputStream(fileInputStream);
    }
  }

  /**
   * Reads avro object from input stream.
   *
   * @param inputStream The input stream to read from
   * @return Avro object
   * @throws IOException
   */
  AvroYarnJobSubmissionParameters fromInputStream(final InputStream inputStream) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
            AvroYarnJobSubmissionParameters.getClassSchema(), inputStream);
    final SpecificDatumReader<AvroYarnJobSubmissionParameters> reader = new SpecificDatumReader<>(
            AvroYarnJobSubmissionParameters.class);
    return reader.read(null, decoder);
  }

}
