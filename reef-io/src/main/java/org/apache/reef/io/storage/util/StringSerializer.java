/**
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

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.io.Accumulable;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.serialization.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StringSerializer implements
    Serializer<String, OutputStream> {
  @Override
  public Accumulable<String> create(OutputStream arg) {
    final DataOutputStream dos = new DataOutputStream(arg);
    return new Accumulable<String>() {

      @Override
      public Accumulator<String> accumulator() throws ServiceException {
        return new Accumulator<String>() {

          @Override
          public void add(String datum) throws ServiceException {
            byte[] b = datum.getBytes();
            try {
              dos.writeInt(b.length);
              dos.write(b);
            } catch (IOException e) {
              throw new ServiceException(e);
            }

          }

          @Override
          public void close() throws ServiceException {
            try {
              dos.close();
            } catch (IOException e) {
              throw new ServiceException(e);
            }
          }
        };
      }
    };
  }
}