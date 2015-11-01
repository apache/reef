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

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;

/**
 * The actual node that is part of the operator topology
 *
 * Receives data from the handlers and provides them to the
 * operators/OperatorTopologyStruct when they need it.
 *
 * This implementation decouples the send and receive.
 */
public interface NodeStruct {

  String getId();

  int getVersion();

  void setVersion(int version);

  byte[] getData();

  void addData(GroupCommunicationMessage msg);
}
