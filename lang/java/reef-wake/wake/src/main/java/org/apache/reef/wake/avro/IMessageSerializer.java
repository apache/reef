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

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.ByteArrayOutputStream;

/**
 * Base interface for Avro message serializer objects.
 */
public interface IMessageSerializer {
  /**
   * Deserialize messages of type TMessage from input outputStream.
   * @param outputStream A ByteArrayOutputStream where the message to
   *                     be serialized will be written.
   * @param message An Avro message class which implements the Avro SpcificRecord interface.
   * @param sequence The numerical position of the message in the outgoing message stream.
   * @throws IOException An error occurred writing the message to the outputStream.
   */
  void serialize(ByteArrayOutputStream outputStream, SpecificRecord message, long sequence)
    throws IOException;
}


