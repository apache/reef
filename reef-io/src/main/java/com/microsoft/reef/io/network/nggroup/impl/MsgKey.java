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

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 *
 */
public class MsgKey {
  private final String src;
  private final String dst;
  private final Type msgType;

  public MsgKey(final String src, final String dst, final Type msgType) {
    this.src = src;
    this.dst = dst;
    this.msgType = msgType;
  }

  public MsgKey(final GroupCommMessage msg) {
    this.src = msg.getSrcid();
    this.dst = msg.getDestid();
    this.msgType = msg.getType();
  }


  public String getSrc() {
    return src;
  }


  public String getDst() {
    return dst;
  }


  public Type getMsgType() {
    return msgType;
  }

  @Override
  public String toString() {
    return "(" + src + "," + dst + "," + msgType + ")";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MsgKey)) {
      return false;
    }
    final MsgKey that = (MsgKey) obj;
    if (!this.src.equals(that.src)) {
      return false;
    }
    if (!this.dst.equals(that.dst)) {
      return false;
    }
    if (!this.msgType.equals(that.msgType)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = src.hashCode();
    result = 31 * result + dst.hashCode();
    result = 31 * result + msgType.hashCode();
    return result;
  }
}
