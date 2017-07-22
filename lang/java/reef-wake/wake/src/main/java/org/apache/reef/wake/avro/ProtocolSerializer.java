/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.wake.avro;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.reef.wake.MultiObserver;
import org.apache.reef.wake.avro.message.Header;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

/**
 * The ProtocolSerializer generates serializers and deserializers for
 * all of the Avro messages contained in a specified package. The name
 * of the package must have "message" as the final component of the
 * package name. For example, Avro messages in the org.foo.me package
 * would sit in the org.foo.me.messages package.
 */
public final class ProtocolSerializer {
  private static final Logger LOG = Logger.getLogger(ProtocolSerializer.class.getName());
  // Maps for mapping message class names to serializer and deserializer classes.
  private final Map<String, IMessageSerializer> nameToSerializerMap = new HashMap<>();
  private final Map<String, IMessageDeserializer> nameToDeserializerMap = new HashMap<>();

  /**
   * Finds all of the messages in the specified packaged and calls register.
   * @param messagePackage A string which contains the full name of the
   *                       package containing the protocol messages.
   */
  public ProtocolSerializer(final String messagePackage) {
    // Build a list of the message reflection classes.
    final ScanResult scanResult = new FastClasspathScanner(messagePackage).scan();
    final List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    final List<Class<?>> messageClasses = scanResult.classNamesToClassRefs(scanNames);

    // Add the header message from the org.apache.reef.wake.avro.message package.
    messageClasses.add(Header.class);

    try {
      // Register all of the messages in the specified package.
      for (final Class<?> cls : messageClasses) {
        this.register(cls);
      }
    } catch (final Exception e) {
      throw new RuntimeException("Message registration failed", e);
    }
  }

  /**
   * Instantiates and adds a message serializer/deserializer for the message.
   * @param msgMetaClass The reflection class for the message.
   * @param <TMessage> The Java type of the message being registered.
   */
  public <TMessage> void register(final Class<TMessage> msgMetaClass) {
    LOG.log(Level.INFO, "Registering message: {0}", msgMetaClass.getSimpleName());
    nameToSerializerMap.put(msgMetaClass.getSimpleName(), SerializationFactory.createSerializer(msgMetaClass));
    nameToDeserializerMap.put(msgMetaClass.getSimpleName(), SerializationFactory.createDeserializer(msgMetaClass));
  }

  /**
   * Marshall the input message to a byte array.
   * @param message The message to be marshaled into a byte array.
   * @param sequence The unique sequence number of the message.
   */
  public byte[] write(final SpecificRecord message, final long sequence) {
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final String name = message.getClass().getSimpleName();
      LOG.log(Level.FINE, "Serializing message: {0}", name);

      final IMessageSerializer serializer = nameToSerializerMap.get(name);
      if (serializer != null) {
        serializer.serialize(outputStream, message, sequence);
      }

      return outputStream.toByteArray();
    } catch (final Exception e) {
      throw new RuntimeException("Failure writing message: " + message.getClass().getCanonicalName(), e);
    }
  }

  /**
   * Read a message from the input byte stream and send it to the event handler.
   * @param messageBytes An array of bytes that contains the message to be deserialized.
   * @param observer An implementation of the MultiObserver interface which will be called
   *                 to process the deserialized message.
   */
  public void read(final byte[] messageBytes, final MultiObserver observer) {
    try (final InputStream inputStream = new ByteArrayInputStream(messageBytes)) {
      // Binary decoder for both the header and the message.
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

      // Read the header message.
      final SpecificDatumReader<Header> headerReader = new SpecificDatumReader<>(Header.class);
      final Header header = headerReader.read(null, decoder);
      LOG.log(Level.FINE, "Deserializing Avro message: {0}", header.getClassName());

      // Get the appropriate deserializer and deserialize the message.
      final IMessageDeserializer deserializer = nameToDeserializerMap.get(header.getClassName().toString());
      if (deserializer != null) {
        deserializer.deserialize(decoder, observer, header.getSequence());
      } else {
        throw new RuntimeException("Request to deserialize unknown message type: " +  header.getClassName());
      }

    } catch (final Exception e) {
      throw new RuntimeException("Failure reading message: ", e);
    }
  }
}
