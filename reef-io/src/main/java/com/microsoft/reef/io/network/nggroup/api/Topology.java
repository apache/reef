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

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.Configuration;

/**
 *
 */
public interface Topology {

  /**
   * @param senderId
   */
  void setRoot(String senderId);

  /**
   * @param spec
   */
  void setOperSpec(OperatorSpec spec);

  /**
   * @param taskId
   * @return
   */
  Configuration getConfig(String taskId);

  /**
   * @param taskId
   */
  void addTask(String taskId);

  /**
   * @param id
   */
  void setFailed(String id);

  /**
   * @param id
   */
  void setRunning(String id);

  /**
   * @param msg
   */
  void processMsg(GroupCommMessage msg);

  /**
   * @param taskId
   */
  void removeTask(String taskId);

  /**
   * @param taskId
   * @return
   */
  boolean isRunning(String taskId);

  /**
   * @param taskId
   * @return
   */
  int getNodeVersion(String taskId);

  /**
   * @return
   */
  String getRootId();

}
