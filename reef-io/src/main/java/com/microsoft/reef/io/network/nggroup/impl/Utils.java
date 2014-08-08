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
package com.microsoft.reef.io.network.nggroup.impl;

import com.google.protobuf.ByteString;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.tang.annotations.Name;

/**
 *
 */
public class Utils {

  /**
   * @param groupName
   * @param operName
   * @param broadcast
   * @param from
   * @param to
   * @param data
   * @return
   */
  /*public static GroupCommMessage bldGCM(
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName, final Type msgType, final String from,
      final String to, final byte[]... data) {
    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setGroupname(groupName.getName());
    GCMBuilder.setOperatorname(operName.getName());
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from);
    GCMBuilder.setDestid(to);

    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    for (final byte[] element : data) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }

    return GCMBuilder.build();
  }
*/

  /**
   * @param groupName
   * @param operName
   * @param broadcast
   * @param from
   * @param to
   * @param data
   * @return
   */
  public static GroupCommMessage bldVersionedGCM(
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final Type msgType,
      final String from,
      final int srcVersion,
      final String to,
      final int dstVersion,
      final byte[]... data) {
    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setGroupname(groupName.getName());
    if (operName != null) {
      GCMBuilder.setOperatorname(operName.getName());
    }
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from);
    GCMBuilder.setSrcVersion(srcVersion);
    GCMBuilder.setDestid(to);
    GCMBuilder.setVersion(dstVersion);

    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    for (final byte[] element : data) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }

    return GCMBuilder.build();
  }

  public static Class<? extends Name<String>> getClass(final String className) {
    try {
      return (Class<? extends Name<String>>) Class.forName(className);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class " + className, e);
    }
  }

  public static String simpleName(final Class<?> className) {
    if (className != null) {
      return className.getSimpleName();
    } else {
      return "NULL";
    }
  }

  public static void main(final String[] args) {
    System.out.println(Utils.class.getSimpleName());
  }

  public static byte[] getData(final GroupCommMessage gcm) {
    return (gcm.getMsgsCount() == 1) ? gcm.getMsgs(0).getData().toByteArray() : null;
  }
}
