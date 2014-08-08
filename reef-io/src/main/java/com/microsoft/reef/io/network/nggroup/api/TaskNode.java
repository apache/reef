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

/**
 *
 */
public interface TaskNode {

  /**
   * @param leaf
   */
  public void addChild(TaskNode child);

  /**
   * @param root
   */
  public void setParent(TaskNode parent);

  /**
   * @return
   */
  public String taskId();

  /**
   * @return
   */
  public boolean isRunning();

  /**
   * @return
   */
  TaskNode getParent();

  /**
   * @param neighborId
   * @return
   */
  boolean isNeighborActive(String neighborId);

  /**
   * @return
   */
  public void chkAndSendTopSetup();

  /**
   * @param source
   */
  public void chkAndSendTopSetup(String source);

  /**
   *
   */
  public void setFailed();

  /**
   *
   */
  public void setRunning();

  /**
   * @param msg
   */
  public void processMsg(GroupCommMessage msg);

  /**
   *
   */
  public void processParentRunning();

  /**
   *
   */
  public void processChildRunning(String childId);

  /**
   *
   */
  public void processChildDead(String childId);

  /**
   *
   */
  public void processParentDead();

  /**
   *
   */
  public void waitForTopologySetupOrFailure();

  /**
   * @return
   */
  public boolean hasChanges();

  /**
   * @return
   */
  boolean resetTopologySetupSent();

  /**
   * @param taskNode
   */
  public void removeChild(TaskNode taskNode);

  /**
   * @return
   */
  public int getVersion();

  /**
   *
   */
  public void waitForUpdatedTopologyOrFailure();

  /**
   * @return
   */
  boolean wasTopologySetupSent();
}
