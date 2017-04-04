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
package org.apache.reef.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyDeserializer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.reef.io.storage.util.ParquetReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

public class ParquetReaderTest {
  @Test
  public void testSchema() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("file.parquet").getFile());
    Path parquetPath = new Path(file.getAbsolutePath());
    ParquetReader reader = new ParquetReader(parquetPath);
    Schema schema = reader.getAvroSchema();
    Assert.assertEquals("User", schema.getName());
    Assert.assertEquals(Schema.Type.RECORD, schema.getType());
  }

  @Test
  public void testDataEntries() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("file.parquet").getFile());
    Path parquetPath = new Path(file.getAbsolutePath());
    ParquetReader reader = new ParquetReader(parquetPath);

    byte[] byteArr = reader.serializeToByteBuffer().array();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArr);
    DatumReader datumReader = new GenericDatumReader<GenericRecord>();
    datumReader.setSchema(reader.getAvroSchema());

    AvroKeyDeserializer deserializer
            = new AvroKeyDeserializer<GenericRecord>(reader.getAvroSchema(), reader.getAvroSchema(), datumReader);
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
