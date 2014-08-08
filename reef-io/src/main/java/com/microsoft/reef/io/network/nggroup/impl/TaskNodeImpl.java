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

import com.microsoft.reef.io.network.nggroup.api.TaskNode;
import com.microsoft.reef.io.network.nggroup.api.TaskNodeStatus;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 */
public class TaskNodeImpl implements TaskNode {

  /**
   *
   */
  private static final byte[] EmptyByteArr = new byte[0];

  private static final Logger LOG = Logger.getLogger(TaskNodeImpl.class.getName());

  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final String driverId;

  private TaskNode parent;
  private final List<TaskNode> children = new ArrayList<>();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean topoSetupSent = new AtomicBoolean(false);

  private final TaskNodeStatus nodeStatus;

  private final Object parentLock = new Object();
  private final Object childrenLock = new Object();

  private final AtomicInteger version = new AtomicInteger(0);

  public TaskNodeImpl(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operatorName,
      final String taskId,
      final String driverId) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.taskId = taskId;
    this.driverId = driverId;
    nodeStatus = new NodeStatusImpl(senderStage, groupName, operatorName, taskId, driverId, this);
  }

  /**
   * * Methods pertaining to my status change ***
   */
  @Override
  public void setFailed() {
    if (!running.compareAndSet(true, false)) {
      LOG.warning(getQualifiedName() + "Trying to set failed on an already failed task. Something fishy!!!");
      return;
    }
    nodeStatus.setFailed();
    LOG.info(getQualifiedName() + "Changed status to failed.");
    LOG.info(getQualifiedName() + "Resetting topoSetupSent to false");
    topoSetupSent.set(false);
    synchronized (parentLock) {
      if (checkParentAliveNKicking()) {
        parent.processChildDead(taskId);
      } else {
        LOG.info(getQualifiedName() + "Skipping asking parent to process child death");
      }
    }
    synchronized (childrenLock) {
      for (final TaskNode child : children) {
        if (!child.isRunning()) {
          LOG.info(getQualifiedName() + child.taskId() + " is not running yet. skipping asking it to process parent death");
          continue;
        }
        child.processParentDead();
      }
    }
    final int version = this.version.incrementAndGet();
    LOG.info(getQualifiedName() + "Bumping up to version-" + version);

  }

  @Override
  public void setRunning() {
    if (!running.compareAndSet(false, true)) {
      LOG.warning(getQualifiedName() + "Trying to set running on an already running task. Something fishy!!!");
      return;
    }
    final int version = this.version.get();
    LOG.info(getQualifiedName() + "Changed status to running version-" + version);
    synchronized (parentLock) {
      if (checkParentAliveNKicking()) {
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ParentAdd, parent.taskId(), parent.getVersion(), taskId,
            version, EmptyByteArr));
        parent.processChildRunning(taskId);
      } else {
        LOG.info(getQualifiedName() + "Skipping src add to & for parent");
      }
    }
    synchronized (childrenLock) {
      for (final TaskNode child : children) {
        if (!child.isRunning()) {
          LOG.info(getQualifiedName() + child.taskId() + " is not running yet. skipping src add send");
          continue;
        }
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ChildAdd, child.taskId(), child.getVersion(), taskId, version, EmptyByteArr));
        child.processParentRunning();
      }
    }
  }

  /**** Methods pertaining to my status change ends ****/

  /**
   * * Methods pertaining to my neighbors status change ***
   */


  private boolean checkParentAliveNKicking() {
    if (parent == null) {
      LOG.warning(getQualifiedName() + "Parent does not exist");
      return false;
    }
    if (!parent.isRunning()) {
      LOG.warning(getQualifiedName() + "Parent is not running");
      return false;
    }
    return true;
  }

  @Override
  public void processParentRunning() {
    if (!running.get()) {
      LOG.warning(getQualifiedName() + "Was running when parent asked me to process its start. But I am not running anymore");
      return;
    }
    LOG.info(getQualifiedName() + "Processing Parent Running");
    synchronized (parentLock) {
      if (checkParentAliveNKicking()) {
        final int parentVersion = parent.getVersion();
        final String parentTaskId = parent.taskId();
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ParentAdd, parentTaskId, parentVersion, taskId, version.get(),
            EmptyByteArr));
      } else {
        LOG.warning(getQualifiedName() + "Parent was running when I was asked to add him."
            + " However, he is not active anymore. Returning without sending ParentAdd"
            + " msg. ***CHECK***");
      }
    }
  }


  @Override
  public void processParentDead() {
    if (!running.get()) {
      LOG.warning(getQualifiedName() + "Was running when parent asked me to process its death. But I am not running anymore");
      return;
    }
    LOG.info(getQualifiedName() + "Processing Parent Death");

    synchronized (parentLock) {
      //Need to grab the lock since
      //parent might start running
      //though unlikely
      if (parent != null) {
        final int parentVersion = parent.getVersion();
        final String parentTaskId = parent.taskId();
        nodeStatus.setFailed(parent.taskId());
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ParentDead, parentTaskId, parentVersion, taskId,
            version.get(), EmptyByteArr));
      } else {
        final String msg = getQualifiedName() + "Don't expect parent to be null. Something wrong";
        LOG.warning(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private TaskNode checkChildAliveNKicking(final String childId) {
    final TaskNode childTask = findChildTask(childId);
    if (childTask == null) {
      LOG.warning(getQualifiedName() + childId + " is not active");
      return null;
    }
    if (!childTask.isRunning()) {
      LOG.warning(getQualifiedName() + childId + " is not running");
      return null;
    }
    return childTask;
  }

  @Override
  public void processChildRunning(final String childId) {
    if (!running.get()) {
      LOG.warning(getQualifiedName()
          + "Was running when a child asked me to process its start. But I am not running anymore");
      return;
    }
    LOG.info(getQualifiedName() + "Processing Child " + childId + " running");

    synchronized (childrenLock) {
      final TaskNode childTask = checkChildAliveNKicking(childId);
      if (childTask != null) {
        final int childVersion = childTask.getVersion();
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ChildAdd, childId, childVersion, taskId, version.get(), EmptyByteArr));
      } else {
        LOG.warning(getQualifiedName()
            + childId + " was running when I was asked to add him."
            + " However, I can't find a task corresponding to him now."
            + " Returning without sending ChildAdd msg. ***CHECK***");
      }
    }
  }


  @Override
  public void processChildDead(final String childId) {
    if (!running.get()) {
      LOG.warning(getQualifiedName() + "Was running when a child asked me to process its death. But I am not running anymore");
      return;
    }
    LOG.info(getQualifiedName() + "Processing Child " + childId + " death");

    synchronized (childrenLock) {
      final TaskNode childTask = findChildTask(childId);
      if (childTask != null) {
        final int childVersion = childTask.getVersion();
        nodeStatus.setFailed(childId);
        nodeStatus.sendMsg(Utils.bldVersionedGCM(groupName, operName,
            Type.ChildDead, childId, childVersion, taskId, version.get(), EmptyByteArr));
      } else {
        final String msg = getQualifiedName() + "Don't expect task for " + childId
            + " to be null. Something wrong";
        LOG.warning(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  /**
   * * Methods pertaining to my neighbors status change ends ***
   */

  @Override
  public void processMsg(final GroupCommMessage msg) {
    if (!msg.hasVersion()) {
      throw new RuntimeException(getQualifiedName() + "NodeStatusMsgProcessorStage can only deal with versioned msgs");
    }
    final int rcvVersion = msg.getSrcVersion();
    final int version = this.version.get();
    if (rcvVersion < version) {
      LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage received a ver-" + rcvVersion
          + " msg while expecting ver-" + version + ". Discarding msg");
      return;
    }
    if (version < rcvVersion) {
      LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage received a HIGHER ver-" + rcvVersion
          + " msg while expecting ver-" + version + ". Something fishy!!!");
      return;
    }
    nodeStatus.processMsg(msg);
  }

  @Override
  public String taskId() {
    return taskId;
  }

  @Override
  public void addChild(final TaskNode child) {
    synchronized (childrenLock) {
      children.add(child);
    }
  }

  @Override
  public void removeChild(final TaskNode child) {
    synchronized (childrenLock) {
      children.remove(child);
    }
  }

  @Override
  public void setParent(final TaskNode parent) {
    synchronized (parentLock) {
      this.parent = parent;
    }
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public TaskNode getParent() {
    synchronized (parentLock) {
      return parent;
    }
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
        + taskId + ":ver(" + getVersion() + ") - ";
  }


  @Override
  public boolean isNeighborActive(final String neighborId) {
    return nodeStatus.isActive(neighborId);
  }

  @Override
  public boolean resetTopologySetupSent() {
    return topoSetupSent.compareAndSet(true, false);
  }

  @Override
  public boolean wasTopologySetupSent() {
    return topoSetupSent.get();
  }

  @Override
  public void chkAndSendTopSetup() {
    LOG.info(getQualifiedName()
        + "Checking if I am ready to send TopoSetup msg");
    if (wasTopologySetupSent()) {
      LOG.info(getQualifiedName() + "topology setup msg sent already");
      return;
    }
    final boolean parentActive = parentActive();
    final boolean allChildrenActive = allChildrenActive();
    if (parentActive && allChildrenActive) {
      final boolean activeNeighborOfParent = activeNeighborOfParent();
      final boolean activeNeighborOfAllChildren = activeNeighborOfAllChildren();
      if (activeNeighborOfParent && activeNeighborOfAllChildren) {
        sendTopoSetupMsg();
        return;
      } else {
        if (!activeNeighborOfParent) {
          LOG.info(getQualifiedName()
              + "I am not an active neighbor of parent "
              + (parent != null ? parent.taskId() : "NULL"));
        }
        if (!activeNeighborOfAllChildren) {
          LOG.info(getQualifiedName()
              + "I am not an active neighbor of all children");
        }
      }
    } else {
      if (!parentActive) {
        LOG.info(getQualifiedName() + "parent "
            + (parent != null ? parent.taskId() : "NULL") + " not active yet");
      }
      if (!allChildrenActive) {
        LOG.info(getQualifiedName() + "not all children active yet");
      }
    }
  }

  private void sendTopoSetupMsg() {
    LOG.info(getQualifiedName() + " Sending TopoSetup msg to " + taskId);
    senderStage.onNext(Utils.bldVersionedGCM(groupName, operName,
        Type.TopologySetup, driverId, 0, taskId, version.get(), new byte[0]));
    nodeStatus.topoSetupSent();
    if (!topoSetupSent.compareAndSet(false, true)) {
      LOG.warning(getQualifiedName() + "TopologySetup msg was sent more than once. Something fishy!!!");
    }
  }

  @Override
  public void chkAndSendTopSetup(final String source) {
    final TaskNode srcNode = findTask(source);
    if (srcNode != null) {
      srcNode.chkAndSendTopSetup();
    } else {
      LOG.warning(getQualifiedName() + "Can't chk topology setup on a null node for task " + source);
    }
  }

  /**
   * @param sourceId
   * @return
   */
  private TaskNode findTask(final String sourceId) {
    synchronized (parentLock) {
      if (parent != null && parent.taskId().equals(sourceId)) {
        return parent;
      }
    }
    return findChildTask(sourceId);
  }

  private TaskNode findChildTask(final String sourceId) {
    synchronized (childrenLock) {
      for (final TaskNode child : children) {
        if (child.taskId().equals(sourceId)) {
          return child;
        }
      }
      return null;
    }
  }

  /**
   * @return
   */
  private boolean parentActive() {
    synchronized (parentLock) {
      if (parent == null) {
        LOG.info(getQualifiedName()
            + "Parent null. Perhaps I am root. A non-existent neghboris always active");
        return true;
      }
      if (!parent.isRunning() || isNeighborActive(parent.taskId())) {
        LOG.info(getQualifiedName() + parent.taskId() + " is an active neghbor");
        return true;
      }
      return false;
    }
  }

  /**
   * @return
   */
  private boolean allChildrenActive() {
    synchronized (childrenLock) {
      for (final TaskNode child : children) {
        final String childId = child.taskId();
        if (child.isRunning() && !isNeighborActive(childId)) {
          LOG.info(getQualifiedName() + childId + " not active yet");
          return false;
        }
      }
    }
    LOG.info(getQualifiedName() + "All children active");
    return true;
  }

  /**
   * @return
   */
  private boolean activeNeighborOfParent() {
    if (parent == null) {
      LOG.info(getQualifiedName() + "Parent null. Perhaps I am root. Always an active neghbor of non-existent parent");
      return true;
    }
    synchronized (parentLock) {
      if (!parent.isRunning() || parent.isNeighborActive(taskId)) {
        LOG.info(getQualifiedName() + "I am an active neighbor of parent "
            + parent.taskId());
        return true;
      }
      return false;
    }
  }

  /**
   * @return
   */
  private boolean activeNeighborOfAllChildren() {
    synchronized (childrenLock) {
      for (final TaskNode child : children) {
        if (child.isRunning() && !child.isNeighborActive(taskId)) {
          LOG.info(getQualifiedName() + "Not an active neighbor of child "
              + child.taskId());
          return false;
        }
      }
    }
    LOG.info(getQualifiedName() + "Active neighbor of all children");
    return true;
  }

  @Override
  public void waitForTopologySetupOrFailure() {
    nodeStatus.waitForTopologySetup();
  }

  @Override
  public void waitForUpdatedTopologyOrFailure() {
    nodeStatus.waitForUpdatedTopologyOrFailure();
  }

  @Override
  public boolean hasChanges() {
    return nodeStatus.hasChanges();
  }

  @Override
  public int getVersion() {
    return version.get();
  }
}
