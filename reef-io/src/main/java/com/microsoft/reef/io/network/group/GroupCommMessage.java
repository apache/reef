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
package com.microsoft.reef.io.network.group;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;

import java.util.Arrays;
import java.util.List;

public class GroupCommMessage<T> {

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final Type msgType;
  private final String from;
  private final int srcVersion;
  private final String to;
  private final int dstVersion;
  private final List<T> data;

  public GroupCommMessage(
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final Type msgType,
      final String from,
      final int srcVersion,
      final String to,
      final int dstVersion,
      final T... data) {

    super();

    this.groupName = groupName;
    this.operName = operName;
    this.msgType = msgType;
    this.from = from;
    this.srcVersion = srcVersion;
    this.to = to;
    this.dstVersion = dstVersion;
    this.data = Arrays.asList(data);
  }

  public Class<? extends Name<String>> getGroupName() {
    return groupName;
  }

  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  public Type getMsgType() {
    return msgType;
  }

  public String getFrom() {
    return from;
  }

  public int getSrcVersion() {
    return srcVersion;
  }

  public String getTo() {
    return to;
  }

  public int getDstVersion() {
    return dstVersion;
  }

  public List<T> getData() {
    return data;
  }
}
