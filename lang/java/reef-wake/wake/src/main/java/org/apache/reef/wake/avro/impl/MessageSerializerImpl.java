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
package org.apache.reef.wake.avro.impl;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.wake.avro.IMessageSerializer;
import org.apache.reef.wake.avro.ProtocolSerializer;
import org.apache.reef.wake.avro.message.Header;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Implementation of Avro message specific serializer.
 * @param <TMessage> Java type of the message the instantiation can serialize.
 */
public class MessageSerializerImpl<TMessage> implements IMessageSerializer {
  private final String msgMetaClassName;
  // Writers for header and message.
  private final DatumWriter<Header> headerWriter = new SpecificDatumWriter<>(Header.class);
  private final DatumWriter<TMessage> messageWriter;

  /**
   * Initialize message specific serializer.
   * @param msgMetaClass The reflection class for the message.
   */
  public MessageSerializerImpl(final Class<TMessage> msgMetaClass) {
    this.msgMetaClassName = ProtocolSerializer.getClassId(msgMetaClass);
    this.messageWriter = new SpecificDatumWriter<>(msgMetaClass);
  }

  /**
   * Deserialize messages of type TMessage from input outputStream.
   * @param outputStream A ByteArrayOutputStream where the message to
   *                     be serialized will be written.
   * @param message An Avro message class which implements the Avro SpcificRecord interface.
   * @param sequence The numerical position of the message in the outgoing message stream.
   * @throws IOException An error occurred writing the message to the outputStream.
   */
  public void serialize(final ByteArrayOutputStream outputStream,
                        final SpecificRecord message, final long sequence) throws IOException {
    // Binary encoder for both the header and message.
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    // Write the header and the message.
    headerWriter.write(new Header(sequence, msgMetaClassName), encoder);
    messageWriter.write((TMessage)message, encoder);
    encoder.flush();
  }
}
