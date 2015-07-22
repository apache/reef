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
package org.apache.reef.io.network.util;

import com.google.protobuf.ByteString;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.wake.Identifier;

public final class TestUtils {
  public static ReefNetworkGroupCommProtos.GroupCommMessage bldGCM(
      final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType,
      final Identifier from, final Identifier to, final byte[]... elements) {
    final ReefNetworkGroupCommProtos.GroupCommMessage.Builder gcmBuilder =
        ReefNetworkGroupCommProtos.GroupCommMessage.newBuilder();
    gcmBuilder.setType(msgType);
    gcmBuilder.setSrcid(from.toString());
    gcmBuilder.setDestid(to.toString());
    final ReefNetworkGroupCommProtos.GroupMessageBody.Builder bodyBuilder =
        ReefNetworkGroupCommProtos.GroupMessageBody.newBuilder();
    for (final byte[] element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      gcmBuilder.addMsgs(bodyBuilder.build());
    }
    final ReefNetworkGroupCommProtos.GroupCommMessage msg = gcmBuilder.build();
    return msg;
  }

  /**
   * @param type
   * @return
   */
  public static boolean controlMessage(final ReefNetworkGroupCommProtos.GroupCommMessage.Type type) {

    switch (type) {
    case AllGather:
    case AllReduce:
    case Broadcast:
    case Gather:
    case Reduce:
    case ReduceScatter:
    case Scatter:
      return false;

    default:
      return true;
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private TestUtils() {
  }
}
