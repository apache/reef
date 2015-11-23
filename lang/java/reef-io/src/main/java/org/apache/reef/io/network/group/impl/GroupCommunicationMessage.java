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
package org.apache.reef.io.network.group.impl;

import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;

import java.util.Arrays;

/**
 * Group Communication Message.
 */
public class GroupCommunicationMessage {
  private final String groupName;
  private final String operName;
  private final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType;
  private final String from;
  private final int srcVersion;
  private final String to;
  private final int dstVersion;
  private final byte[][] data;

  private final String simpleGroupName;
  private final String simpleOperName;

  public GroupCommunicationMessage(
      final String groupName,
      final String operName,
      final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType,
      final String from, final int srcVersion,
      final String to, final int dstVersion,
      final byte[][] data) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.msgType = msgType;
    this.from = from;
    this.srcVersion = srcVersion;
    this.to = to;
    this.dstVersion = dstVersion;
    this.data = data;
    this.simpleGroupName = Utils.simpleName(Utils.getClass(groupName));
    this.simpleOperName = Utils.simpleName(Utils.getClass(operName));
  }

  public String getGroupname() {
    return groupName;
  }

  public String getOperatorname() {
    return operName;
  }

  public String getSimpleOperName() {
    return simpleOperName;
  }

  public ReefNetworkGroupCommProtos.GroupCommMessage.Type getType() {
    return msgType;
  }

  public String getSrcid() {
    return from;
  }

  public int getSrcVersion() {
    return srcVersion;
  }

  public String getDestid() {
    return to;
  }

  public int getVersion() {
    return dstVersion;
  }

  public String getSource() {
    return "(" + getSrcid() + "," + getSrcVersion() + ")";
  }

  public String getDestination() {
    return "(" + getDestid() + "," + getVersion() + ")";
  }

  public byte[][] getData() {
    return data;
  }

  public int getMsgsCount() {
    return data.length;
  }

  public boolean hasVersion() {
    return true;
  }

  public boolean hasSrcVersion() {
    return true;
  }

  @Override
  public String toString() {
    return "[" + msgType + " from " + getSource() + " to " + getDestination() + " for " + simpleGroupName + ":" +
        simpleOperName + "]";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this != obj) {
      if (obj instanceof GroupCommunicationMessage) {
        final GroupCommunicationMessage that = (GroupCommunicationMessage) obj;
        if (!this.groupName.equals(that.groupName)) {
          return false;
        }
        if (!this.operName.equals(that.operName)) {
          return false;
        }
        if (!this.from.equals(that.from)) {
          return false;
        }
        if (this.srcVersion != that.srcVersion) {
          return false;
        }
        if (!this.to.equals(that.to)) {
          return false;
        }
        if (this.dstVersion != that.dstVersion) {
          return false;
        }
        if (!this.msgType.equals(that.msgType)) {
          return false;
        }
        if (this.data.length != that.data.length) {
          return false;
        }
        for (int i = 0; i < data.length; i++) {
          if (!Arrays.equals(this.data[i], that.data[i])) {
            return false;
          }
        }

        return true;
      } else {
        return false;
      }
    } else {
      return true;
    }

  }

  @Override
  public int hashCode() {
    int result = groupName.hashCode();
    result = 31 * result + operName.hashCode();
    result = 31 * result + msgType.hashCode();
    result = 31 * result + from.hashCode();
    result = 31 * result + srcVersion;
    result = 31 * result + to.hashCode();
    result = 31 * result + dstVersion;
    result = 31 * result + Arrays.deepHashCode(data);
    return result;
  }
}
