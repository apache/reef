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
package com.microsoft.reef.services.storage;

import com.microsoft.reef.exception.evaluator.ServiceException;
import com.microsoft.reef.io.Accumulator;
import com.microsoft.reef.io.storage.FramingInputStream;
import com.microsoft.reef.io.storage.FramingOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class FramingTest {

  @Test
  public void frameRoundTripTest() throws IOException, ServiceException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    FramingOutputStream o = new FramingOutputStream(baos);
    FramingOutputStream o2 = new FramingOutputStream(baos2);
    Accumulator<byte[]> a = o2.accumulator();
    int offset = 0;
    for(int i = 0; i < 256; i++ ) {
      byte[] b = new byte[i];
      Arrays.fill(b, (byte)i);
      o.write(b);
      if(i == 255) {
        o.close();
      } else {
        o.nextFrame();
      }
      offset += (4 + i);
      Assert.assertEquals(offset, o.getCurrentOffset());
      a.add(b);
      Assert.assertEquals(offset, o2.getCurrentOffset());
    }
    a.close();
    o2.close();
    byte[] b1 = baos.toByteArray();
    byte[] b2 = baos2.toByteArray();
    Assert.assertArrayEquals(b1,  b2);
    FramingInputStream inA1 = new FramingInputStream(new ByteArrayInputStream(b1));
    FramingInputStream inA2 = new FramingInputStream(new ByteArrayInputStream(b2));
    for(int i = 0; i <= 256; i++ ) {
      byte[] b = new byte[i];
      Arrays.fill(b, (byte)i);
      byte[] f = inA1.readFrame();
      byte[] g = inA2.readFrame();
      if(i == 256) {
        Assert.assertNull(f);
        Assert.assertNull(g);
      } else {
        Assert.assertArrayEquals(b, f);
        Assert.assertArrayEquals(b, g);
      }
    }
    inA2.close();
    inA1.close();

    FramingInputStream inB1 = new FramingInputStream(new ByteArrayInputStream(b1));
    int i = 0;
    for(byte[] bin : inB1) {
      byte[] b = new byte[i];
      Arrays.fill(b, (byte)i);
      Assert.assertArrayEquals(b, bin);
      i++;
    }
    Assert.assertEquals(256, i);
    inB1.close();

    FramingInputStream inB2 = new FramingInputStream(new ByteArrayInputStream(b2));
    i = 0;
    for(byte[] bin : inB2) {
      byte[] b = new byte[i];
      Arrays.fill(b, (byte)i);
      Assert.assertArrayEquals(b, bin);
      i++;
    }
    Assert.assertEquals(256, i);
    inB2.close();
    Assert.assertArrayEquals(b1, b2);
  }

}
