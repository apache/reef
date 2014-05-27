/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.storage.util;

import com.microsoft.reef.exception.evaluator.ServiceException;
import com.microsoft.reef.io.Accumulable;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.serialization.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class IntegerSerializer implements
    Serializer<Integer, OutputStream> {
  @Override
  public Accumulable<Integer> create(OutputStream arg) {
    final DataOutputStream dos = new DataOutputStream(arg);
    return new Accumulable<Integer>() {

      @Override
      public Accumulator<Integer> accumulator() throws ServiceException {
        return new Accumulator<Integer>() {
          @Override
          public void add(Integer datum) throws ServiceException {
            try {
              dos.writeInt(datum);
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