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
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.MultiObserver;
import org.apache.reef.wake.avro.message.Header;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

import javax.inject.Inject;

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

  private final SpecificDatumReader<Header> headerReader = new SpecificDatumReader<>(Header.class);

  /**
   * Finds all of the messages in the specified packaged and calls register.
   * @param messagePackage A string which contains the full name of the
   *                       package containing the protocol messages.
   */
  @Inject
  private ProtocolSerializer(
      @Parameter(ProtocolSerializerNamespace.class) final String messagePackage) {

    // Build a list of the message reflection classes.
    final ScanResult scanResult = new FastClasspathScanner(messagePackage).scan();
    final List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    final List<Class<?>> messageClasses = scanResult.classNamesToClassRefs(scanNames);

    // Add the header message from the org.apache.reef.wake.avro.message package.
    messageClasses.add(Header.class);

    // Register all of the messages in the specified package.
    for (final Class<?> cls : messageClasses) {
      this.register(cls);
    }
  }

  /**
   * Get a canonical string ID of the class. This ID is then used as a key to find
   * serializer and deserializer of the message payload. We need a separate method
   * for it to make sure all parties use the same algorithm to get the class ID.
   * @param clazz class of the message to be serialized/deserialized.
   * @return canonical string ID of the class.
   */
  public static String getClassId(final Class<?> clazz) {
    return clazz.getCanonicalName();
  }

  /**
   * Instantiates and adds a message serializer/deserializer for the message.
   * @param msgMetaClass The reflection class for the message.
   * @param <TMessage> The Java type of the message being registered.
   */
  public <TMessage> void register(final Class<TMessage> msgMetaClass) {
    final String classId = getClassId(msgMetaClass);
    LOG.log(Level.INFO, "Registering message: {0}", classId);
    this.nameToSerializerMap.put(classId, SerializationFactory.createSerializer(msgMetaClass));
    this.nameToDeserializerMap.put(classId, SerializationFactory.createDeserializer(msgMetaClass));
  }

  /**
   * Marshall the input message to a byte array.
   * @param message The message to be marshaled into a byte array.
   * @param sequence The unique sequence number of the message.
   */
  public byte[] write(final SpecificRecord message, final long sequence) {

    final String classId = getClassId(message.getClass());
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

      LOG.log(Level.INFO, "Serializing message: {0}", classId);

      final IMessageSerializer serializer = this.nameToSerializerMap.get(classId);
      if (serializer != null) {
        serializer.serialize(outputStream, message, sequence);
      }

      return outputStream.toByteArray();

    } catch (final IOException e) {
      throw new RuntimeException("Failure writing message: " + classId, e);
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
      final Header header = this.headerReader.read(null, decoder);
      final String classId = header.getClassName().toString();
      LOG.log(Level.INFO, "Deserializing Avro message: {0}", classId);

      // Get the appropriate deserializer and deserialize the message.
      final IMessageDeserializer deserializer = this.nameToDeserializerMap.get(classId);
      if (deserializer != null) {
        deserializer.deserialize(decoder, observer, header.getSequence());
      } else {
        throw new RuntimeException("Request to deserialize unknown message type: " + classId);
      }

    } catch (final IOException e) {
      throw new RuntimeException("Failure reading message", e);
    } catch (final InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Error deserializing message body", e);
    }
  }
}
