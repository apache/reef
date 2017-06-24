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
package org.apache.reef.experimental.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyDeserializer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

public class ParquetReaderTest {

  private final File file = new File(getClass().getClassLoader().getResource("file.parquet").getFile());

  @Test
  public void testSchema() throws IOException, InjectionException {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder();
    builder.bindNamedParameter(PathString.class, file.getAbsolutePath());
    final Configuration conf = builder.build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    final ParquetReader reader = injector.getInstance(ParquetReader.class);
    final Schema schema = reader.createAvroSchema();

    Assert.assertEquals("User", schema.getName());
    Assert.assertEquals(Schema.Type.RECORD, schema.getType());
  }

  @Test
  public void testDataEntries() throws IOException, InjectionException {
    final JavaConfigurationBuilder builder = Tang.Factory.getTang().newConfigurationBuilder();
    builder.bindNamedParameter(PathString.class, file.getAbsolutePath());
    final Configuration conf = builder.build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    final ParquetReader reader = injector.getInstance(ParquetReader.class);

    final byte[] byteArr = reader.serializeToByteBuffer().array();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArr);
    final DatumReader datumReader = new GenericDatumReader<GenericRecord>();
    datumReader.setSchema(reader.createAvroSchema());

    final AvroKeyDeserializer deserializer
            = new AvroKeyDeserializer<GenericRecord>(reader.createAvroSchema(), reader.createAvroSchema(), datumReader);
    deserializer.open(inputStream);

    AvroWrapper<GenericRecord> record = null;

    for (int i = 0; i < 10; i = i + 1) {
      record = deserializer.deserialize(record);
      Assert.assertEquals("User_" + i, record.datum().get("name").toString());
      Assert.assertEquals(i, record.datum().get("age"));
      Assert.assertEquals("blue", record.datum().get("favorite_color").toString());
    }
  }
}
