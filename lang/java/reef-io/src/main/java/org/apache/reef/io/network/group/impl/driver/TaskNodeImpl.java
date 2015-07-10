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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.api.driver.TaskNode;
import org.apache.reef.io.network.group.api.driver.TaskNodeStatus;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EStage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class TaskNodeImpl implements TaskNode {

  private static final Logger LOG = Logger.getLogger(TaskNodeImpl.class.getName());

  private final EStage<GroupCommunicationMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final String driverId;

  private final boolean isRoot;
  private TaskNode parent;
  private TaskNode sibling;
  private final List<TaskNode> children = new ArrayList<>();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean topoSetupSent = new AtomicBoolean(false);

  private final TaskNodeStatus taskNodeStatus;

  private final AtomicInteger version = new AtomicInteger(0);

  public TaskNodeImpl(final EStage<GroupCommunicationMessage> senderStage,
                      final Class<? extends Name<String>> groupName,
                      final Class<? extends Name<String>> operatorName,
                      final String taskId, final String driverId, final boolean isRoot) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operatorName;
    this.taskId = taskId;
    this.driverId = driverId;
    this.isRoot = isRoot;
    taskNodeStatus = new TaskNodeStatusImpl(groupName, operatorName, taskId, this);
  }

  @Override
  public void setSibling(final TaskNode leaf) {
    LOG.entering("TaskNodeImpl", "setSibling", new Object[]{getQualifiedName(), leaf});
    sibling = leaf;
    LOG.exiting("TaskNodeImpl", "setSibling", getQualifiedName());
  }

  @Override
  public int getNumberOfChildren() {
    LOG.entering("TaskNodeImpl", "getNumberOfChildren", getQualifiedName());
    final int size = children.size();
    LOG.exiting("TaskNodeImpl", "getNumberOfChildren", getQualifiedName() + size);
    return size;
  }

  @Override
  public TaskNode successor() {
    LOG.entering("TaskNodeImpl", "successor", getQualifiedName());
    LOG.exiting("TaskNodeImpl", "successor", getQualifiedName() + sibling);
    return sibling;
  }

  @Override
  public String toString() {
    return "(" + taskId + "," + version.get() + ")";
  }

  /**
   * * Methods pertaining to my status change ***.
   */
  @Override
  public void onFailedTask() {
    LOG.entering("TaskNodeImpl", "onFailedTask", getQualifiedName());
    if (!running.compareAndSet(true, false)) {
      LOG.fine(getQualifiedName() + "Trying to set failed on an already failed task. Something fishy!!!");
      LOG.exiting("TaskNodeImpl", "onFailedTask", getQualifiedName() +
          "Trying to set failed on an already failed task. Something fishy!!!");
      return;
    }
    taskNodeStatus.clearStateAndReleaseLocks();
    LOG.finest(getQualifiedName() + "Changed status to failed.");
    LOG.finest(getQualifiedName() + "Resetting topoSetupSent to false");
    topoSetupSent.set(false);
    if (parent != null && parent.isRunning()) {
      parent.onChildDead(taskId);
    } else {
      LOG.finest(getQualifiedName() + "Skipping asking parent to process child death");
    }
    for (final TaskNode child : children) {
      if (child.isRunning()) {
        child.onParentDead();
      }
    }
    final int version = this.version.incrementAndGet();
    LOG.finest(getQualifiedName() + "Bumping up to version-" + version);
    LOG.exiting("TaskNodeImpl", "onFailedTask", getQualifiedName());
  }

  @Override
  public void onRunningTask() {
    LOG.entering("TaskNodeImpl", "onRunningTask", getQualifiedName());
    if (!running.compareAndSet(false, true)) {
      LOG.fine(getQualifiedName() + "Trying to set running on an already running task. Something fishy!!!");
      LOG.exiting("TaskNodeImpl", "onRunningTask", getQualifiedName() +
          "Trying to set running on an already running task. Something fishy!!!");
      return;
    }
    final int version = this.version.get();
    LOG.finest(getQualifiedName() + "Changed status to running version-" + version);
    if (parent != null && parent.isRunning()) {
      final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentAdd, parent.getTaskId(),
          parent.getVersion(), taskId,
          version, Utils.EMPTY_BYTE_ARR);
      taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
      senderStage.onNext(gcm);
      parent.onChildRunning(taskId);
    } else {
      LOG.finest(getQualifiedName() + "Skipping src add to & for parent");
    }
    for (final TaskNode child : children) {
      if (child.isRunning()) {
        final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
            ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, child.getTaskId(),
            child.getVersion(), taskId, version,
            Utils.EMPTY_BYTE_ARR);
        taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
        senderStage.onNext(gcm);
        child.onParentRunning();
      }
    }
    LOG.exiting("TaskNodeImpl", "onRunningTask", getQualifiedName());
  }

  /**
   * * Methods pertaining to my status change ends ***.
   */

  @Override
  public void onParentRunning() {
    LOG.entering("TaskNodeImpl", "onParentRunning", getQualifiedName());
    if (parent != null && parent.isRunning()) {
      final int parentVersion = parent.getVersion();
      final String parentTaskId = parent.getTaskId();
      final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentAdd, parentTaskId,
          parentVersion, taskId, version.get(),
          Utils.EMPTY_BYTE_ARR);
      taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
      senderStage.onNext(gcm);
    } else {
      LOG.finer(getQualifiedName() + "Parent was running when I was asked to add him."
          + " However, he is not active anymore. Returning without sending ParentAdd" + " msg. ***CHECK***");
    }
    LOG.exiting("TaskNodeImpl", "onParentRunning", getQualifiedName());
  }

  @Override
  public void onParentDead() {
    LOG.entering("TaskNodeImpl", "onParentDead", getQualifiedName());
    if (parent != null) {
      final int parentVersion = parent.getVersion();
      final String parentTaskId = parent.getTaskId();
      taskNodeStatus.updateFailureOf(parent.getTaskId());
      final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentDead, parentTaskId,
          parentVersion, taskId, version.get(),
          Utils.EMPTY_BYTE_ARR);
      taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
      senderStage.onNext(gcm);
    } else {
      throw new RuntimeException(getQualifiedName() + "Don't expect parent to be null. Something wrong");
    }
    LOG.exiting("TaskNodeImpl", "onParentDead", getQualifiedName());
  }

  @Override
  public void onChildRunning(final String childId) {
    LOG.entering("TaskNodeImpl", "onChildRunning", new Object[]{getQualifiedName(), childId});
    final TaskNode childTask = findTask(childId);
    if (childTask != null && childTask.isRunning()) {
      final int childVersion = childTask.getVersion();
      final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdd, childId,
          childVersion, taskId, version.get(),
          Utils.EMPTY_BYTE_ARR);
      taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
      senderStage.onNext(gcm);
    } else {
      LOG.fine(getQualifiedName() + childId + " was running when I was asked to add him."
          + " However, I can't find a task corresponding to him now."
          + " Returning without sending ChildAdd msg. ***CHECK***");
    }
    LOG.exiting("TaskNodeImpl", "onChildRunning", getQualifiedName() + childId);
  }

  @Override
  public void onChildDead(final String childId) {
    LOG.entering("TaskNodeImpl", "onChildDead", new Object[]{getQualifiedName(), childId});
    final TaskNode childTask = findChildTask(childId);
    if (childTask != null) {
      final int childVersion = childTask.getVersion();
      taskNodeStatus.updateFailureOf(childId);
      final GroupCommunicationMessage gcm = Utils.bldVersionedGCM(groupName, operName,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead, childId,
          childVersion, taskId, version.get(),
          Utils.EMPTY_BYTE_ARR);
      taskNodeStatus.expectAckFor(gcm.getType(), gcm.getSrcid());
      senderStage.onNext(gcm);
    } else {
      throw new RuntimeException(getQualifiedName() + "Don't expect task for " + childId +
          " to be null. Something wrong");
    }
    LOG.exiting("TaskNodeImpl", "onChildDead", getQualifiedName() + childId);
  }

  /**
   * * Methods pertaining to my neighbors status change ends ***.
   */

  @Override
  public void onReceiptOfAcknowledgement(final GroupCommunicationMessage msg) {
    LOG.entering("TaskNodeImpl", "onReceiptOfAcknowledgement", new Object[]{getQualifiedName(), msg});
    taskNodeStatus.processAcknowledgement(msg);
    LOG.exiting("TaskNodeImpl", "onReceiptOfAcknowledgement", getQualifiedName() + msg);
  }

  @Override
  public void updatingTopology() {
    LOG.entering("TaskNodeImpl", "updatingTopology", getQualifiedName());
    taskNodeStatus.updatingTopology();
    LOG.exiting("TaskNodeImpl", "updatingTopology", getQualifiedName());
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public void addChild(final TaskNode child) {
    LOG.entering("TaskNodeImpl", "addChild", new Object[]{getQualifiedName(), child.getTaskId()});
    children.add(child);
    LOG.exiting("TaskNodeImpl", "addChild", getQualifiedName() + child);
  }

  @Override
  public void removeChild(final TaskNode child) {
    LOG.entering("TaskNodeImpl", "removeChild", new Object[]{getQualifiedName(), child.getTaskId()});
    children.remove(child);
    LOG.exiting("TaskNodeImpl", "removeChild", getQualifiedName() + child);
  }

  @Override
  public void setParent(final TaskNode parent) {
    LOG.entering("TaskNodeImpl", "setParent", new Object[]{getQualifiedName(), parent});
    this.parent = parent;
    LOG.exiting("TaskNodeImpl", "setParent", getQualifiedName() + parent);
  }

  @Override
  public boolean isRunning() {
    LOG.entering("TaskNodeImpl", "isRunning", getQualifiedName());
    final boolean b = running.get();
    LOG.exiting("TaskNodeImpl", "isRunning", getQualifiedName() + b);
    return b;
  }

  @Override
  public TaskNode getParent() {
    LOG.entering("TaskNodeImpl", "getParent", getQualifiedName());
    LOG.exiting("TaskNodeImpl", "getParent", getQualifiedName() + parent);
    return parent;
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":(" + taskId + "," + getVersion() + ") - ";
  }

  @Override
  public boolean isNeighborActive(final String neighborId) {
    LOG.entering("TaskNodeImpl", "isNeighborActive", new Object[]{getQualifiedName(), neighborId});
    final boolean active = taskNodeStatus.isActive(neighborId);
    LOG.exiting("TaskNodeImpl", "isNeighborActive", getQualifiedName() + active);
    return active;
  }

  @Override
  public boolean resetTopologySetupSent() {
    LOG.entering("TaskNodeImpl", "resetTopologySetupSent", new Object[]{getQualifiedName(), });
    final boolean retVal = topoSetupSent.compareAndSet(true, false);
    LOG.exiting("TaskNodeImpl", "resetTopologySetupSent", getQualifiedName() + retVal);
    return retVal;
  }

  @Override
  public void checkAndSendTopologySetupMessage() {
    LOG.entering("TaskNodeImpl", "checkAndSendTopologySetupMessage", getQualifiedName());
    if (!topoSetupSent.get()
        && (parentActive() && activeNeighborOfParent())
        && (allChildrenActive() && activeNeighborOfAllChildren())) {
      sendTopoSetupMsg();
    }
    LOG.exiting("TaskNodeImpl", "checkAndSendTopologySetupMessage", getQualifiedName());
  }

  private void sendTopoSetupMsg() {
    LOG.entering("TaskNodeImpl", "sendTopoSetupMsg", getQualifiedName() + taskId);
    LOG.fine(getQualifiedName() + "is an active participant in the topology");
    senderStage.onNext(Utils.bldVersionedGCM(groupName, operName,
        ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologySetup, driverId, 0, taskId,
        version.get(), Utils.EMPTY_BYTE_ARR));
    taskNodeStatus.onTopologySetupMessageSent();
    final boolean sentAlready = !topoSetupSent.compareAndSet(false, true);
    if (sentAlready) {
      LOG.fine(getQualifiedName() + "TopologySetup msg was sent more than once. Something fishy!!!");
    }
    LOG.exiting("TaskNodeImpl", "sendTopoSetupMsg", getQualifiedName());
  }

  @Override
  public void checkAndSendTopologySetupMessageFor(final String source) {
    LOG.entering("TaskNodeImpl", "checkAndSendTopologySetupMessageFor", new Object[]{getQualifiedName(), source});
    final TaskNode srcNode = findTask(source);
    if (srcNode != null) {
      srcNode.checkAndSendTopologySetupMessage();
    }
    LOG.exiting("TaskNodeImpl", "checkAndSendTopologySetupMessageFor", getQualifiedName() + source);
  }

  /**
   * @param sourceId
   * @return
   */
  private TaskNode findTask(final String sourceId) {
    LOG.entering("TaskNodeImpl", "findTask", new Object[]{getQualifiedName(), sourceId});
    final TaskNode retNode;
    if (parent != null && parent.getTaskId().equals(sourceId)) {
      retNode = parent;
    } else {
      retNode = findChildTask(sourceId);
    }
    LOG.exiting("TaskNodeImpl", "findTask", getQualifiedName() + retNode);
    return retNode;
  }

  private TaskNode findChildTask(final String sourceId) {
    LOG.entering("TaskNodeImpl", "findChildTask", new Object[]{getQualifiedName(), sourceId});
    TaskNode retNode = null;
    for (final TaskNode child : children) {
      if (child.getTaskId().equals(sourceId)) {
        retNode = child;
        break;
      }
    }
    LOG.exiting("TaskNodeImpl", "findChildTask", getQualifiedName() + retNode);
    return retNode;
  }

  private boolean parentActive() {
    LOG.entering("TaskNodeImpl", "parentActive", getQualifiedName());
    if (isRoot) {
      LOG.exiting("TaskNodeImpl", "parentActive",
          Arrays.toString(new Object[]{true, getQualifiedName(),
              "I am root. Will never have parent. So signalling active"}));
      return true;
    }
    if (isNeighborActive(parent.getTaskId())) {
      LOG.exiting("TaskNodeImpl", "parentActive",
          Arrays.toString(new Object[]{true, getQualifiedName(), parent, " is an active neighbor"}));
      return true;
    }
    LOG.exiting("TaskNodeImpl", "parentActive",
        getQualifiedName() + "Neither root Nor is " + parent + " an active neighbor");
    return false;
  }

  private boolean activeNeighborOfParent() {
    LOG.entering("TaskNodeImpl", "activeNeighborOfParent", getQualifiedName());
    if (isRoot) {
      LOG.exiting("TaskNodeImpl", "activeNeighborOfParent", Arrays.toString(new Object[]{true, getQualifiedName(),
          "I am root. Will never have parent. So signalling active"}));
      return true;
    }
    if (parent.isNeighborActive(taskId)) {
      LOG.exiting("TaskNodeImpl", "activeNeighborOfParent", Arrays.toString(new Object[]{true, getQualifiedName(),
          "I am an active neighbor of parent ", parent}));
      return true;
    }
    LOG.exiting("TaskNodeImpl", "activeNeighborOfParent", Arrays.toString(new Object[]{false, getQualifiedName(),
        "Neither is parent null Nor am I an active neighbor of parent ", parent}));
    return false;
  }

  private boolean allChildrenActive() {
    LOG.entering("TaskNodeImpl", "allChildrenActive", getQualifiedName());
    for (final TaskNode child : children) {
      final String childId = child.getTaskId();
      if (child.isRunning() && !isNeighborActive(childId)) {
        LOG.exiting("TaskNodeImpl", "allChildrenActive",
            Arrays.toString(new Object[]{false, getQualifiedName(), childId, " not active yet"}));
        return false;
      }
    }
    LOG.exiting("TaskNodeImpl", "allChildrenActive",
        Arrays.toString(new Object[]{true, getQualifiedName(), "All children active"}));
    return true;
  }

  private boolean activeNeighborOfAllChildren() {
    LOG.entering("TaskNodeImpl", "activeNeighborOfAllChildren", getQualifiedName());
    for (final TaskNode child : children) {
      if (child.isRunning() && !child.isNeighborActive(taskId)) {
        LOG.exiting("TaskNodeImpl", "activeNeighborOfAllChildren",
            Arrays.toString(new Object[]{false, getQualifiedName(), "Not an active neighbor of child ", child}));
        return false;
      }
    }
    LOG.exiting("TaskNodeImpl", "activeNeighborOfAllChildren",
        Arrays.toString(new Object[]{true, getQualifiedName(), "Active neighbor of all children"}));
    return true;
  }

  @Override
  public void waitForTopologySetupOrFailure() {
    LOG.entering("TaskNodeImpl", "waitForTopologySetupOrFailure", getQualifiedName());
    taskNodeStatus.waitForTopologySetup();
    LOG.exiting("TaskNodeImpl", "waitForTopologySetupOrFailure", getQualifiedName());
  }

  @Override
  public boolean hasChanges() {
    LOG.entering("TaskNodeImpl", "hasChanges", getQualifiedName());
    final boolean changes = taskNodeStatus.hasChanges();
    LOG.exiting("TaskNodeImpl", "hasChanges", getQualifiedName() + changes);
    return changes;
  }

  @Override
  public int getVersion() {
    return version.get();
  }

  @Override
  public int hashCode() {
    int r = taskId.hashCode();
    r = 31 * r + version.get();
    return r;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj != this) {
      if (obj instanceof TaskNodeImpl) {
        final TaskNodeImpl that = (TaskNodeImpl) obj;
        return (this.taskId.equals(that.taskId) && this.version.get() == that.version.get());
      } else {
        return false;
      }
    } else {
      return true;
    }
  }
}
