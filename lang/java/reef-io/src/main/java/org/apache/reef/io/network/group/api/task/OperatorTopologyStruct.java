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
package org.apache.reef.io.network.group.api.task;

import org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.operators.Sender;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The actual local topology maintaining the
 * children and parent that reacts to update
 * and data msgs. The actual nodes are represented
 * by NodeStruct and it handles receiving and
 * providing data
 */
public interface OperatorTopologyStruct {

  Class<? extends Name<String>> getGroupName();

  Class<? extends Name<String>> getOperName();

  String getSelfId();

  int getVersion();

  NodeStruct getParent();

  Collection<? extends NodeStruct> getChildren();

  String getDriverId();

  Sender getSender();

  boolean hasChanges();

  void setChanges(boolean b);

  void addAsData(GroupCommunicationMessage msg);

  void update(Set<GroupCommunicationMessage> deletionDeltas);

  void update(GroupCommunicationMessage msg);

  void sendToParent(byte[] data, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType);

  byte[] recvFromParent(ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType);

  void sendToChildren(byte[] data, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType);

  void sendToChildren(Map<String, byte[]> dataMap, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType);

  <T> T recvFromChildren(ReduceFunction<T> redFunc, Codec<T> dataCodec);

  byte[] recvFromChildren();
}
