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

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Serialize Drive information.
 * It is the default implementation for interface DriverInfoSerializer.
 */
public class AvroDriverInfoSerializer implements DriverInfoSerializer {

  @Inject
  AvroDriverInfoSerializer() {
  }

  /**
   * Build AvroDriverInfo object.
   */
  @Override
  public AvroDriverInfo toAvro(final String id, final String startTime, final List<AvroReefServiceInfo> services) {
    return AvroDriverInfo.newBuilder()
        .setRemoteId(id)
        .setStartTime(startTime)
        .setServices(services)
        .build();
  }

  /**
   * Convert AvroDriverInfo object to JSON string.
   */
  @Override
  public String toString(final AvroDriverInfo avroDriverInfo) {
    final DatumWriter<AvroDriverInfo> driverWriter = new SpecificDatumWriter<>(AvroDriverInfo.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroDriverInfo.getSchema(), out);
      driverWriter.write(avroDriverInfo, encoder);
      encoder.flush();
      return out.toString(AvroHttpSerializer.JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
