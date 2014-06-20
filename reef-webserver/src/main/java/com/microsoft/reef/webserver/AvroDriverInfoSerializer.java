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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serialize Drive information
 */
public class AvroDriverInfoSerializer {
  public AvroDriverInfo toAvro(String id, String startTime) {
    return AvroDriverInfo.newBuilder()
        .setRemoteId(id)
        .setStartTime(startTime)
        .build();
  }

  public String toString(final AvroDriverInfo avroDriverInfo) {
    final DatumWriter<AvroDriverInfo> evaluatorWriter = new SpecificDatumWriter<>(AvroDriverInfo.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroDriverInfo.getSchema(), out);
      evaluatorWriter.write(avroDriverInfo, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
