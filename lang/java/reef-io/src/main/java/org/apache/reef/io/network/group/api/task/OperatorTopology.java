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

import org.apache.reef.io.network.exception.ParentDeadException;
import org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;

import java.util.Map;

/**
 * Represents the local topology of tasks for an operator. It
 * provides methods to send/rcv from parents and children
 * <p>
 * Every operator is an {@code EventHandler<GroupCommunicationMessage>}
 * and it will use an instance of this type to delegate the
 * handling of the message and also uses it to communicate
 * with its parents and children
 * <p>
 * This is an operator facing interface. The actual topology is
 * maintained in OperatorTopologyStruct. Current strategy is to
 * maintain two versions of the topology and current operations
 * are always delegated to effectiveTopology and the baseTopology
 * is updated while initialization and when user calls updateTopology.
 * So this is only a wrapper around the two versions of topologies
 * and manages when to create/update them based on the messages from
 * the driver.
 */
public interface OperatorTopology {

  void handle(GroupCommunicationMessage msg);

  void sendToParent(byte[] encode, ReefNetworkGroupCommProtos.GroupCommMessage.Type reduce) throws ParentDeadException;

  byte[] recvFromParent(ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws ParentDeadException;

  void sendToChildren(byte[] data, ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws ParentDeadException;

  void sendToChildren(Map<String, byte[]> dataMap,
                      ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) throws ParentDeadException;

  <T> T recvFromChildren(ReduceFunction<T> redFunc, Codec<T> dataCodec) throws ParentDeadException;

  byte[] recvFromChildren() throws ParentDeadException;

  void initialize() throws ParentDeadException;
}
