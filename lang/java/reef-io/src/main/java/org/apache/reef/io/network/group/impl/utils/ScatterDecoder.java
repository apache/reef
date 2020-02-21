/*
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
package org.apache.reef.io.network.group.impl.utils;

import org.apache.reef.wake.remote.Decoder;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decode messages that was created by {@code ScatterEncoder}.
 */
public final class ScatterDecoder implements Decoder<ScatterData> {
  private static final Logger LOG = Logger.getLogger(ScatterDecoder.class.getName());

  @Inject
  ScatterDecoder() {
  }

  public ScatterData decode(final byte[] data) {
    try (DataInputStream dstream = new DataInputStream(new ByteArrayInputStream(data))) {
      final int elementCount = dstream.readInt();

      // first read data that I should receive
      final byte[][] myData = new byte[elementCount][];
      for (int index = 0; index < elementCount; index++) {
        final int encodedElementLength = dstream.readInt();
        myData[index] =  new byte[encodedElementLength];
        if (dstream.read(myData[index]) == -1) {
          LOG.log(Level.FINE, "No data read because end of stream was reached");
        }
      }

      // and then read the data intended for my children
      final Map<String, byte[]> childDataMap = new HashMap<>();
      while (dstream.available() > 0) {
        final String childId = dstream.readUTF();
        final byte[] childData = new byte[dstream.readInt()];
        if (dstream.read(childData) == -1) {
          LOG.log(Level.FINE, "No data read because end of stream was reached");
        }
        childDataMap.put(childId, childData);
      }

      return new ScatterData(myData, childDataMap);

    } catch (final IOException e) {
      throw new RuntimeException("IOException", e);
    }
  }
}
