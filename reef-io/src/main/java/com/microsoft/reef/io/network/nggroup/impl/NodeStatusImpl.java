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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 */
public class NodeStatusImpl implements TaskNodeStatus {

  private static final Logger LOG = Logger.getLogger(NodeStatusImpl.class.getName());

  private final ConcurrentCountingMap<Type, String> statusMap = new ConcurrentCountingMap<>();
  private final EStage<GroupCommMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final String driverId;
  private final Set<String> activeNeighbors = new HashSet<>();
  private final CountingMap<String> neighborStatus = new CountingMap<>();
  private final AtomicBoolean topoSetupRcvd = new AtomicBoolean(false);
  private final Object topoUpdateStageLock = new Object();
  private final Object topoSetupSentLock = new Object();
  private final TaskNode node;


  public NodeStatusImpl(
      final EStage<GroupCommMessage> senderStage,
      final Class<? extends Name<String>> groupName,
      final Class<? extends Name<String>> operName,
      final String taskId,
      final String driverId,
      final TaskNode node) {
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.taskId = taskId;
    this.driverId = driverId;
    this.node = node;
  }

  private boolean isDeadMsg(final Type msgAcked) {
    return msgAcked == Type.ParentDead || msgAcked == Type.ChildDead;
  }

  private boolean isAddMsg(final Type msgAcked) {
    return msgAcked == Type.ParentAdd || msgAcked == Type.ChildAdd;
  }

  private Type getAckedMsg(final Type msgType) {
    switch (msgType) {
      case ParentAdded:
        return Type.ParentAdd;
      case ChildAdded:
        return Type.ChildAdd;
      case ParentRemoved:
        return Type.ParentDead;
      case ChildRemoved:
        return Type.ChildDead;
      default:
        return msgType;
    }
  }

  private void chkAndSendTopoSetup(
      final Type msgDealt) {
    LOG.info(getQualifiedName() + "Checking if I am ready to send TopoSetup msg");
    if (statusMap.isEmpty()) {
      LOG.info(getQualifiedName() + "Empty status map.");
      node.chkAndSendTopSetup();
    } else {
      LOG.info(getQualifiedName() + "Status map non-empty" + statusMap);
    }
  }

  @Override
  public void topoSetupSent() {
    neighborStatus.clear();
    synchronized (topoSetupSentLock) {
      topoSetupSentLock.notifyAll();
    }
  }

  @Override
  public boolean isActive(final String neighborId) {
    return activeNeighbors.contains(neighborId);
  }

  @Override
  public boolean amIActive() {
    return statusMap.isEmpty();
  }

  /**
   * This needs to happen in line rather than in
   * a stage because we need to note the messages
   * we send to the tasks before we start processing
   * msgs from the nodes.(Acks & Topology msgs)
   */
  @Override
  public void sendMsg(final GroupCommMessage gcm) {
    final String srcId = gcm.getSrcid();
    final Type msgType = gcm.getType();
    LOG.info(getQualifiedName() + "Handling " + msgType + " msg from " + srcId);
//    LOG.info(getQualifiedName() + "Acquired statusMap lock");
    LOG.info(getQualifiedName() + "Adding " + srcId + " to sources");
    statusMap.add(msgType, srcId);
    LOG.info(getQualifiedName() + "Sources for " + msgType + " are: " + statusMap.get(msgType));
    LOG.info(getQualifiedName() + "Sending " + msgType + " msg from (" + srcId
        + "," + gcm.getSrcVersion() + ") to (" + taskId + "," + gcm.getVersion() + ")");
    senderStage.onNext(gcm);
  }

  @Override
  public void setFailed() {
    statusMap.clear();
    activeNeighbors.clear();
    neighborStatus.clear();
    synchronized (topoSetupSentLock) {
      topoSetupSentLock.notifyAll();
    }
    synchronized (topoUpdateStageLock) {
      topoUpdateStageLock.notifyAll();
    }
  }

  @Override
  public void setFailed(final String taskId) {
    activeNeighbors.remove(taskId);
    neighborStatus.remove(taskId);
  }

  @Override
  public void processMsg(final GroupCommMessage gcm) {
    final String self = gcm.getSrcid();
    LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage handling " + gcm.getType() + " msg from " + self + " for " + gcm.getDestid());
    final Type msgType = gcm.getType();
    final Type msgAcked = getAckedMsg(msgType);
    final String sourceId = gcm.getDestid();
    switch (msgType) {
      case UpdateTopology:
      /*//UpdateTopology might be queued up
      //but the task might have died. If so,
      //do not send this msg
      if(!node.isRunning()) {
        break;
      }*/
        //Send to taskId because srcId is usually always MasterTask
        LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage Sending UpdateTopology msg to " + taskId);
        senderStage.onNext(Utils.bldVersionedGCM(groupName,
            operName, Type.UpdateTopology,
            driverId, 0, taskId, node.getVersion(), new byte[0]));
        break;
      case TopologySetup:
        synchronized (topoUpdateStageLock) {
          topoSetupRcvd.set(true);
          topoUpdateStageLock.notifyAll();
        }
        break;
      case ParentAdded:
      case ChildAdded:
      case ParentRemoved:
      case ChildRemoved:
        if (statusMap.containsKey(msgAcked)) {
          if (statusMap.contains(msgAcked, sourceId)) {
            LOG.info(getQualifiedName() + "NodeStatusMsgProcessorStage Removing " + sourceId +
                " from sources expecting ACK");
            statusMap.remove(msgAcked, sourceId);

            if (isAddMsg(msgAcked)) {
              neighborStatus.add(sourceId);
            } else if (isDeadMsg(msgAcked)) {
              neighborStatus.remove(sourceId);
            }
            if (statusMap.notContains(sourceId)) {
              if (neighborStatus.get(sourceId) > 0) {
                activeNeighbors.add(sourceId);
                node.chkAndSendTopSetup(sourceId);
              } else {
                LOG.info(getQualifiedName() + sourceId + " is not a neighbor anymore");
              }
            } else {
              LOG.info(getQualifiedName() + "Not done processing " + sourceId + " acks yet. So it is still inactive");
            }
            chkAndSendTopoSetup(msgAcked);
          } else {
            LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage Got " + msgType +
                " from a source(" + sourceId + ") to whom ChildAdd was not sent. " +
                "Perhaps reset during failure. If context not indicative use ***CAUTION***");
          }
        } else {
          LOG.warning(getQualifiedName() + "NodeStatusMsgProcessorStage There were no " + msgAcked +
              " msgs sent in the previous update cycle. " +
              "Perhaps reset during failure. If context not indicative use ***CAUTION***");
        }
        break;

      default:
        LOG.warning(getQualifiedName() + "Non-ctrl msg " + gcm.getType() + " for " + gcm.getDestid() +
            " unexpected");
        break;
    }
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
        + taskId + ":ver(" + node.getVersion() + ") - ";
  }

  @Override
  public boolean hasChanges() {
    return !statusMap.isEmpty();
  }

  @Override
  public void waitForTopologySetup() {
    synchronized (topoUpdateStageLock) {
      while (!topoSetupRcvd.get() && node.isRunning()) {
        try {
          topoUpdateStageLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException in NodeTopologyUpdateWaitStage " +
              "while waiting for receiving TopologySetup", e);
        }
      }
      topoSetupRcvd.set(false);
    }
  }

  @Override
  public void waitForUpdatedTopologyOrFailure() {
    synchronized (topoSetupSentLock) {
      while (node.isRunning() && !node.wasTopologySetupSent()) {
        try {
          topoSetupSentLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException(
              "InterruptedException while waiting for sending of TopoSetup", e);
        }
      }
      //topologySetup should not be reset.
      //That happens only when there is
      //failure or there is an UpdateTopology
      //request.
    }
  }

}
