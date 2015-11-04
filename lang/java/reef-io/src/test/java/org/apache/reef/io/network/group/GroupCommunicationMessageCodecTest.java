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
package org.apache.reef.io.network.group;

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessageCodec;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

/**
 *
 */
public class GroupCommunicationMessageCodecTest {

  @NamedParameter
  class GroupName implements Name<String> {
  }

  @NamedParameter
  class OperName implements Name<String> {
  }

  @Test(timeout = 1000)
  public final void testInstantiation() throws InjectionException {
    final GroupCommunicationMessageCodec codec =
        Tang.Factory.getTang().newInjector().getInstance(GroupCommunicationMessageCodec.class);
    Assert.assertNotNull("tang.getInstance(GroupCommunicationMessageCodec.class): ", codec);
  }

  @Test(timeout = 1000)
  public final void testEncodeDecode() {
    final Random r = new Random();
    final byte[] data = new byte[100];
    r.nextBytes(data);
    final GroupCommunicationMessage expMsg = Utils.bldVersionedGCM(GroupName.class, OperName.class,
        ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, "From", 0, "To", 1, data);
    final GroupCommunicationMessageCodec codec = new GroupCommunicationMessageCodec();
    final GroupCommunicationMessage actMsg1 = codec.decode(codec.encode(expMsg));
    Assert.assertEquals("decode(encode(msg)): ", expMsg, actMsg1);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream daos = new DataOutputStream(baos);
    codec.encodeToStream(expMsg, daos);
    final GroupCommunicationMessage actMsg2 =
        codec.decodeFromStream(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
    Assert.assertEquals("decodeFromStream(encodeToStream(msg)): ", expMsg, actMsg2);
  }
}
