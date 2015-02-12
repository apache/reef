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
package org.apache.reef.io.network.group;

import org.apache.reef.io.network.group.impl.GCMCodec;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.network.util.TestUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public class GCMCodecTest {

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.GCMCodec#GCMCodec()}.
   *
   * @throws InjectionException
   */
  @Test(timeout = 1000)
  public final void testGCMCodec() throws InjectionException {
    final GCMCodec codec = Tang.Factory.getTang().newInjector().getInstance(GCMCodec.class);
    Assert.assertNotNull("tang.getInstance(GCMCodec.class)", codec);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.GCMCodec#decode(byte[])}.
   */
  @Test(timeout = 1000)
  public final void testDecode() {
    final ReefNetworkGroupCommProtos.GroupCommMessage expected = TestUtils.bldGCM(
        ReefNetworkGroupCommProtos.GroupCommMessage.Type.Scatter,
        new StringIdentifierFactory().getNewInstance("Task1"),
        new StringIdentifierFactory().getNewInstance("Task2"), "Hello".getBytes());
    final byte[] msgBytes = expected.toByteArray();
    final GCMCodec codec = new GCMCodec();
    final ReefNetworkGroupCommProtos.GroupCommMessage decoded = codec.decode(msgBytes);
    Assert.assertEquals("GCMCodec.decode():", expected, decoded);
  }

  /**
   * Test method for {@link org.apache.reef.io.network.group.impl.GCMCodec#encode(org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage)}.
   */
  @Test(timeout = 1000)
  public final void testEncode() {
    final ReefNetworkGroupCommProtos.GroupCommMessage msg = TestUtils.bldGCM(
        ReefNetworkGroupCommProtos.GroupCommMessage.Type.Scatter,
        new StringIdentifierFactory().getNewInstance("Task1"),
        new StringIdentifierFactory().getNewInstance("Task2"), "Hello".getBytes());
    final byte[] expected = msg.toByteArray();
    final GCMCodec codec = new GCMCodec();
    final byte[] encoded = codec.encode(msg);
    Assert.assertArrayEquals("GCMCodec.encode():", expected, encoded);
  }
}
