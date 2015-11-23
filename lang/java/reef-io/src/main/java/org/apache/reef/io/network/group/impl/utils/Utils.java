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

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.annotations.Name;

import java.util.Iterator;

/**
 * Utility class for group communications.
 */
public final class Utils {

  public static final byte[] EMPTY_BYTE_ARR = new byte[0];

  public static GroupCommunicationMessage bldVersionedGCM(final Class<? extends Name<String>> groupName,
                                                          final Class<? extends Name<String>> operName,
                                                          final ReefNetworkGroupCommProtos.GroupCommMessage.Type
                                                              msgType,
                                                          final String from, final int srcVersion,
                                                          final String to, final int dstVersion, final byte[]... data) {

    return new GroupCommunicationMessage(groupName.getName(), operName.getName(), msgType, from, srcVersion, to,
        dstVersion, data);
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

  public static byte[] getData(final GroupCommunicationMessage gcm) {
    return (gcm.getMsgsCount() == 1) ? gcm.getData()[0] : null;
  }

  /**
   * Extract a group communication message object from a message.
   * @param msg
   * @return
   */
  public static GroupCommunicationMessage getGCM(final Message<GroupCommunicationMessage> msg) {
    final Iterator<GroupCommunicationMessage> gcmIterator = msg.getData().iterator();
    if (gcmIterator.hasNext()) {
      final GroupCommunicationMessage gcm = gcmIterator.next();
      if (gcmIterator.hasNext()) {
        throw new RuntimeException("Expecting exactly one GCM object inside Message but found more");
      }
      return gcm;
    } else {
      throw new RuntimeException("Expecting exactly one GCM object inside Message but found none");
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Utils() {
  }
}
