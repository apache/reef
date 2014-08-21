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
package com.microsoft.reef.services.network.util;

import com.google.protobuf.ByteString;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.wake.Identifier;

public class TestUtils {
  public static GroupCommMessage bldGCM(final Type msgType, final Identifier from, final Identifier to, final byte[]... elements) {
    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());
    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    for (final byte[] element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }
    final GroupCommMessage msg = GCMBuilder.build();
    return msg;
  }

  /**
   * @param type
   * @return
   */
  public static boolean controlMessage(final GroupCommMessage.Type type) {
    switch(type){
    /*case SourceAdd:
    case SourceDead:
    case ParentAdd:
    case ParentDead:
    case ChildAdd:
    case ChildDead:
    case ChildRemoved:
    case ParentRemoved:
    case ChildAdded:
    case ParentAdded:
    case TopologyChanges:
    case TopologySetup:
    case TopologyUpdated:
    case UpdateTopology:
      return true;*/

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
}
