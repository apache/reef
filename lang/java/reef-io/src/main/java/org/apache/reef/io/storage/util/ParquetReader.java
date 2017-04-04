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
package org.apache.reef.io.storage.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

/**
 * A reader for Parquet files that can serialize data to local disk and return Avro schema and Avro reader.
 * The intent is not to build a general parquet reader, but to consume data with table-like property.
 */
public class ParquetReader {
  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(ParquetReader.class.getName());
  
  private Path parquetFilePath;

  @Inject
  public ParquetReader(final Path path) throws IOException {
    parquetFilePath = path;
    Schema schema = getAvroSchema();
    if (schema.getType() != Schema.Type.RECORD) {
      LOG.log(Level.SEVERE, "ParquetReader only support Avro record type that can be consumed as a table.");
    }
    for (Schema.Field f : schema.getFields()) {
      if (f.schema().getType() == Schema.Type.RECORD) {
        LOG.log(Level.SEVERE, "ParquetReader doesn't support nested record type for its elements.");
      }
    }
  }
  
  /**
   * Retrieve avro schema from parquet file.
   * @return avro schema from parquet file.
   */
  public Schema getAvroSchema() throws IOException {
    return getAvroSchema(new Configuration(true), NO_FILTER);
  }
  
  /**
   * Retrieve avro schema from parquet file.
   * @param configuration Hadoop configuration.
   * @param filter Filter for Avro metadata.
   * @return avro schema from parquet file.
   */
  public Schema getAvroSchema(final Configuration configuration, final MetadataFilter filter) throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, parquetFilePath, filter);
    AvroSchemaConverter converter = new AvroSchemaConverter();
    MessageType schema = footer.getFileMetaData().getSchema();
    return converter.convert(schema);
  }
  
  /**
   * Construct an avro reader from parquet file.
   * @return avro reader based on the provided parquet file.
   */
  public AvroParquetReader<GenericRecord> getAvroReader() throws IOException {
    return new AvroParquetReader<GenericRecord>(parquetFilePath);
  }
  
  /**
   * Serialize Avro data to a local file.
   * @param file Local destination file for serialization.
   */
  public void serializeToDisk(final File file) throws IOException {
    DatumWriter datumWriter = new GenericDatumWriter<GenericRecord>();
    DataFileWriter fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    AvroParquetReader<GenericRecord> reader = getAvroReader();
    fileWriter.create(getAvroSchema(), file);
    GenericRecord record = reader.read();
    while (record != null) {
      fileWriter.append(record);
      record = reader.read();
    }
    reader.close();
    fileWriter.close();
  }
  
  /**
   * Serialize Avro data to a in-memory ByteBuffer.
   * @return A ByteBuffer that contains avro data.
   */
  public ByteBuffer serializeToByteBuffer() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    DatumWriter writer = new GenericDatumWriter<GenericRecord>();
    writer.setSchema(getAvroSchema());
    AvroParquetReader<GenericRecord> reader = getAvroReader();
    GenericRecord record = reader.read();
    while (record != null) {
      writer.write(record, encoder);
      record = reader.read();
    }
    reader.close();
    encoder.flush();
    ByteBuffer buf = ByteBuffer.wrap(stream.toByteArray());
    buf.order(ByteOrder.LITTLE_ENDIAN);
    return buf;
  }
}
