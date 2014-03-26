package com.microsoft.reef.io.network.naming.serialization;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utilities for AVRO.
 */
final class AvroUtils {

  private AvroUtils() {
  }

  /**
   * Serializes the given avro object to a byte[]
   *
   * @param avroObject
   * @param theClass
   * @param <T>
   * @return
   */
  static final <T> byte[] toBytes(T avroObject, Class<T> theClass) {
    final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(theClass);
    final byte[] theBytes;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      datumWriter.write(avroObject, encoder);
      encoder.flush();
      out.flush();
      theBytes = out.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException("Unable to serialize an avro object", e);
    }
    return theBytes;
  }

  static <T> T fromBytes(final byte[] theBytes, final Class<T> theClass) {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<T> reader = new SpecificDatumReader<>(theClass);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize an avro object", e);
    }
  }
}
