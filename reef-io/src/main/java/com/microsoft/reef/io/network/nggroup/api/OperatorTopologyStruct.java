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
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.impl.Sender;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 *
 */
public interface OperatorTopologyStruct {

  /**
   * @param msg
   */
  void addAsData(GroupCommMessage msg);

  /**
   * @param deletionDeltas
   */
  void update(Set<GroupCommMessage> deletionDeltas);

  /**
   * @return
   */
  boolean hasChanges();

  /**
   * @param msg
   */
  void update(GroupCommMessage msg);

  /**
   * @return
   */
  String getSelfId();

  /**
   * @return
   */
  String getDriverId();

  /**
   * @return
   */
  Sender getSender();

  /**
   * @return
   */
  Class<? extends Name<String>> getGroupName();

  /**
   * @return
   */
  Class<? extends Name<String>> getOperName();

  /**
   * @param data
   * @param msgType
   */
  void sendToParent(byte[] data, Type msgType);

  /**
   * @param data
   * @param msgType
   */
  void sendToChildren(byte[] data, Type msgType);

  /**
   * @return
   */
  byte[] recvFromParent();

  /**
   * @return
   */
  List<byte[]> recvFromChildren();

  /**
   * @param b
   */
  void setChanges(boolean b);

  /**
   * @return
   */
  NodeStruct getParent();

  /**
   * @return
   */
  Collection<? extends NodeStruct> getChildren();

  /**
   * @param redFunc
   * @param dataCodec
   * @return
   */
  <T> T recvFromChildren(ReduceFunction<T> redFunc, Codec<T> dataCodec);


}
