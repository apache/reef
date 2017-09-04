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

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.reef.wake.MultiObserver;
import org.apache.reef.wake.avro.IMessageDeserializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Implementation of Avro message specific deserializer.
 * @param <TMessage> Java type of the message the instantiation can deserialize.
 */
public class MessageDeserializerImpl<TMessage> implements IMessageDeserializer {
  protected Class<TMessage> msgMetaClass;
  private final SpecificDatumReader<TMessage> messageReader;

  /**
   * Initialize message specific deserializer.
   * @param msgMetaClass The reflection class for the message.
   * @param msgMetaClass
   */
  public MessageDeserializerImpl(final Class<TMessage> msgMetaClass) {
    this.msgMetaClass = msgMetaClass;
    this.messageReader = new SpecificDatumReader<>(msgMetaClass);
  }

  /**
   * Deserialize messages of type TMessage from input decoder.
   * @param decoder An Avro BinaryDecoder instance that is reading the input stream containing the message.
   * @param observer An instance of the MultiObserver class that will process the message.
   * @param sequence A long value which contains the sequence number of the message in the input stream.
   * @throws IOException Read of input stream in decoder failed.
   * @throws IllegalAccessException Target method in observer is not accessible.
   * @throws InvocationTargetException Subclass threw and exception.
   */
  public void deserialize(final BinaryDecoder decoder, final MultiObserver observer, final long sequence)
    throws IOException, IllegalAccessException, InvocationTargetException {

    final TMessage message = messageReader.read(null, decoder);
    if (message != null) {
      observer.onNext(sequence, message);
    } else {
      throw new RuntimeException("Failed to deserialize message [" + msgMetaClass.getSimpleName() + "]");
    }
  }
}
