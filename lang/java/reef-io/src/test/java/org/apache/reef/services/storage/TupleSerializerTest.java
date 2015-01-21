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
package org.apache.reef.services.storage;

import org.apache.reef.exception.evaluator.ServiceException;
import org.apache.reef.io.Accumulator;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.serialization.Deserializer;
import org.apache.reef.io.serialization.Serializer;
import org.apache.reef.io.storage.FramingTupleDeserializer;
import org.apache.reef.io.storage.FramingTupleSerializer;
import org.apache.reef.io.storage.util.IntegerDeserializer;
import org.apache.reef.io.storage.util.IntegerSerializer;
import org.apache.reef.io.storage.util.StringDeserializer;
import org.apache.reef.io.storage.util.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TupleSerializerTest {

  private Serializer<Integer, OutputStream> keySerializer;
  private Serializer<String, OutputStream> valSerializer;
  private Deserializer<Integer, InputStream> keyDeserializer;
  private Deserializer<String, InputStream> valDeserializer;
  private FramingTupleSerializer<Integer, String> fts;
  private ByteArrayOutputStream baos;
  private FramingTupleDeserializer<Integer, String> ftd;
  private Iterable<Tuple<Integer, String>> iterable;

  @Before
  public void setup() throws ServiceException {

    keySerializer = new IntegerSerializer();
    valSerializer = new StringSerializer();
    keyDeserializer = new IntegerDeserializer();
    valDeserializer = new StringDeserializer();

    fts = new FramingTupleSerializer<Integer, String>(
        keySerializer, valSerializer);

    baos = new ByteArrayOutputStream();
    Accumulator<Tuple<Integer, String>> acc = fts.create(baos).accumulator();
    for (int i = 0; i < 100; i++) {
      acc.add(new Tuple<>(i, i + ""));
    }
    acc.close();

    ftd = new FramingTupleDeserializer<Integer, String>(
        keyDeserializer, valDeserializer);
    iterable = ftd.create(new ByteArrayInputStream(baos.toByteArray()));
  }

  @Test
  public void testFramingSerializer() throws ServiceException, IOException {
    int i = 0;
    for (Tuple<Integer, String> t : iterable) {
      Tuple<Integer, String> u = new Tuple<>(i, i + "");
      Assert.assertEquals(u, t);
      i++;
    }
    Assert.assertEquals(100, i);
  }

  @Test(expected = NoSuchElementException.class)
  public void testReadOffEnd() {
    Iterator<Tuple<Integer, String>> it = iterable.iterator();
    try {
      while (it.hasNext()) {
        it.next();
        it.hasNext();
      }
    } catch (NoSuchElementException e) {
      throw new IllegalStateException("Errored out too early!", e);
    }
    it.next();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCantRemove() {
    Iterator<Tuple<Integer, String>> it = iterable.iterator();
    it.next();
    it.remove();
  }
}
