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
package org.apache.reef.bridge;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.bridge.message.Header;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

/**
 * The Serializer is a class utility for serializing and deserialing bridge
 * specific Java to C# messages on the Java side.
 */
public final class Serializer {
  private static final Logger LOG = Logger.getLogger(Serializer.class.getName());
  // Maps for mapping message class names to serializer and deserializer classes.
  private static Map<String, MessageSerializer> nameToSerializerMap = new HashMap<>();
  private static Map<String, MessageDeserializer> nameToDeserializerMap = new HashMap<>();

  /**
   *  Class utility with all static methods.
   */
  private Serializer() { }

  /**
   * Finds all of the messages in the org.apache.reef.bridge.message package and calls register.
   */
  public static void initialize() {
    LOG.log(Level.INFO, "Start: Serializer.Initialize");

    // Build a list of the message reflection classes.
    final ScanResult scanResult = new FastClasspathScanner("org.apache.reef.bridge.message").scan();
    final List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    final List<Class<?>> messageClasses = scanResult.classNamesToClassRefs(scanNames);
    LOG.log(Level.INFO, "!!!NUMBER OF MESSAGES = " + Integer.toString(messageClasses.size()));

    try {
      // Register all of the messages.
      for (Class<?> cls : messageClasses) {
        LOG.log(Level.INFO, "Found message class: " + cls.getName() + " " + cls.getSimpleName());
        Method register = Serializer.class.getMethod("register", cls.getClass());
        LOG.log(Level.INFO, "Obtained the method class instance");
        register.invoke(null, cls);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to register message class " + e.getMessage());
    }
    printMessageMaps();

    LOG.log(Level.INFO, "End: Serializer.Initialize");
  }

  /**
   * Instantiates and adds a message serializer/deserializer for the message.
   * @param msgMetaClass The reflection class for the message.
   * @param <TMessage> The message Java type.
   */
  public static <TMessage> void register(final Class<TMessage> msgMetaClass) {
    LOG.log(Level.INFO, "Registering [" + msgMetaClass.getSimpleName() + "]");

    // Instantiate an anonymous instance of the message serializer for this message type.
    final MessageSerializer messageSerializer = new GenericMessageSerializer<TMessage>(msgMetaClass) {

      public void serialize(final ByteArrayOutputStream outputStream,
                            final SpecificRecord message, final long identifier) throws IOException {
        // Binary encoder for both the header and message.
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // Writers for header and message.
        final DatumWriter<Header> headerWriter = new SpecificDatumWriter<>(Header.class);
        final DatumWriter<TMessage> messageWriter = new SpecificDatumWriter<>(msgMetaClass);

        // Write the header and the message.
        headerWriter.write(new Header(identifier, msgMetaClass.getSimpleName()), encoder);
        messageWriter.write((TMessage)message, encoder);
        encoder.flush();
      }

    };
    nameToSerializerMap.put(msgMetaClass.getSimpleName(), messageSerializer);

    // Instantiate an anonymous instance of the message deserializer for this message type.
    final MessageDeserializer messageDeserializer = new GenericMessageDeserializer<TMessage>(msgMetaClass) {

      public void deserialize(final BinaryDecoder decoder, final MultiObserver observer,
                              final long identifier) throws Exception {
        final SpecificDatumReader<TMessage> messageReader = new SpecificDatumReader<>(msgMetaClass);
        final TMessage message = messageReader.read(null, decoder);
        if (message != null) {
          observer.onNext(identifier, message);
        } else {
          throw new Exception("Failed to deserialize message [" + msgMetaClass.getSimpleName() + "]");
        }
      }

    };
    nameToDeserializerMap.put(msgMetaClass.getSimpleName(), messageDeserializer);
  }

  /**
   * Print the current deserializer message map for debug purposes.
   */
  private static void printMessageMaps() {
    Set<Map.Entry<String, MessageDeserializer>> deserializers = nameToDeserializerMap.entrySet();
    for (Map.Entry<String, MessageDeserializer> entry : deserializers) {
      LOG.log(Level.INFO, "Deserializer entry: " + entry.getKey() + " : "
          + ((entry.getValue() != null) ? entry.getValue().getClass().toString() : "NULL"));
    }
  }

  /**
   * Marshall the input message to a byte array.
   * @param message The message to be marshaled into a byte array.
   * @param
   */
  public static byte[] write(final SpecificRecord message, final long identifier) {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final String name = message.getClass().getSimpleName();
      LOG.log(Level.INFO, "Serializing message: [" + name + "]");

      final MessageSerializer serializer = nameToSerializerMap.get(name);
      serializer.serialize(outputStream, message, identifier);

      return outputStream.toByteArray();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to serialize avro message: " + e.getMessage());
      return null;
    }
  }

  /**
   * Read a message from the input byte stream and send it to the event handler.
   * @param messageBytes An array of bytes that contains the message to be deserialized.
   * @param observer An implementation of the MultiObserver interface.
   */
  public static void read(final byte[] messageBytes, final MultiObserver observer) {
    try (InputStream inputStream = new ByteArrayInputStream(messageBytes)) {
      // Binary decoder for both the header and the message.
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

      // Read the header message.
      final SpecificDatumReader<Header> headerReader = new SpecificDatumReader<>(Header.class);
      final Header header = headerReader.read(null, decoder);
      LOG.log(Level.INFO, "Deserializing message: [" + header.getClassName() + "]");

      // Get the appropriate deserializer and deserialize the message.
      final MessageDeserializer deserializer = nameToDeserializerMap.get(header.getClassName().toString());
      if (deserializer != null) {
        LOG.log(Level.INFO, "Calling message deserializer");
        deserializer.deserialize(decoder, observer, header.getIdentifier());
      } else {
        LOG.log(Level.SEVERE, "Filed to find message deserializer");
      }

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to deserialize avro message: " + e.getMessage());
    }
  }
}
