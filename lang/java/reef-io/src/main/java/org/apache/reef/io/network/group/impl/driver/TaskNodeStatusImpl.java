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
import org.apache.reef.io.network.group.impl.utils.ConcurrentCountingMap;
import org.apache.reef.io.network.group.impl.utils.CountingMap;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import org.apache.reef.tang.annotations.Name;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class TaskNodeStatusImpl implements TaskNodeStatus {

  private static final Logger LOG = Logger.getLogger(TaskNodeStatusImpl.class.getName());

  private final ConcurrentCountingMap<Type, String> statusMap = new ConcurrentCountingMap<>();
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String taskId;
  private final Set<String> activeNeighbors = new HashSet<>();
  private final CountingMap<String> neighborStatus = new CountingMap<>();
  private final AtomicBoolean updatingTopo = new AtomicBoolean(false);
  private final Object topoUpdateStageLock = new Object();
  private final Object topoSetupSentLock = new Object();
  private final TaskNode node;

  public TaskNodeStatusImpl(final Class<? extends Name<String>> groupName,
                            final Class<? extends Name<String>> operName, final String taskId, final TaskNode node) {
    this.groupName = groupName;
    this.operName = operName;
    this.taskId = taskId;
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

  private void chkIamActiveToSendTopoSetup(final Type msgDealt) {
    LOG.entering("TaskNodeStatusImpl", "chkAndSendTopoSetup", new Object[]{getQualifiedName(), msgDealt});
    if (statusMap.isEmpty()) {
      LOG.finest(getQualifiedName() + "Empty status map.");
      node.checkAndSendTopologySetupMessage();
    } else {
      LOG.finest(getQualifiedName() + "Status map non-empty" + statusMap);
    }
    LOG.exiting("TaskNodeStatusImpl", "chkAndSendTopoSetup", getQualifiedName() + msgDealt);
  }

  @Override
  public void onTopologySetupMessageSent() {
    LOG.entering("TaskNodeStatusImpl", "onTopologySetupMessageSent", getQualifiedName());
    neighborStatus.clear();
    synchronized (topoSetupSentLock) {
      topoSetupSentLock.notifyAll();
    }
    LOG.exiting("TaskNodeStatusImpl", "onTopologySetupMessageSent", getQualifiedName());
  }

  @Override
  public boolean isActive(final String neighborId) {
    LOG.entering("TaskNodeStatusImpl", "isActive", new Object[]{getQualifiedName(), neighborId});
    final boolean contains = activeNeighbors.contains(neighborId);
    LOG.exiting("TaskNodeStatusImpl", "isActive", getQualifiedName() + contains);
    return contains;
  }

  /**
   * This needs to happen in line rather than in a stage because we need to note.
   * the messages we send to the tasks before we start processing msgs from the
   * nodes.(Acks & Topology msgs)
   */
  @Override
  public void expectAckFor(final Type msgType, final String srcId) {
    LOG.entering("TaskNodeStatusImpl", "expectAckFor", new Object[]{getQualifiedName(), msgType, srcId});
    LOG.finest(getQualifiedName() + "Adding " + srcId + " to sources");
    statusMap.add(msgType, srcId);
    LOG.exiting("TaskNodeStatusImpl", "expectAckFor", getQualifiedName() + "Sources from which ACKs for " + msgType + " are expected: " + statusMap.get(msgType));
  }

  @Override
  public void clearStateAndReleaseLocks() {
    LOG.entering("TaskNodeStatusImpl", "clearStateAndReleaseLocks", getQualifiedName());
    statusMap.clear();
    activeNeighbors.clear();
    neighborStatus.clear();
    updatingTopo.compareAndSet(true, false);
    synchronized (topoSetupSentLock) {
      topoSetupSentLock.notifyAll();
    }
    synchronized (topoUpdateStageLock) {
      topoUpdateStageLock.notifyAll();
    }
    LOG.exiting("TaskNodeStatusImpl", "clearStateAndReleaseLocks", getQualifiedName());
  }

  @Override
  public void updateFailureOf(final String taskId) {
    LOG.entering("TaskNodeStatusImpl", "updateFailureOf", new Object[]{getQualifiedName(), taskId});
    activeNeighbors.remove(taskId);
    neighborStatus.remove(taskId);
    LOG.exiting("TaskNodeStatusImpl", "updateFailureOf", getQualifiedName());
  }

  @Override
  public void processAcknowledgement(final GroupCommunicationMessage gcm) {
    LOG.entering("TaskNodeStatusImpl", "processMsg", new Object[]{getQualifiedName(), gcm});
    final String self = gcm.getSrcid();
    final Type msgType = gcm.getType();
    final Type msgAcked = getAckedMsg(msgType);
    final String sourceId = gcm.getDestid();
    switch (msgType) {
    case TopologySetup:
      synchronized (topoUpdateStageLock) {
        if (!updatingTopo.compareAndSet(true, false)) {
          LOG.fine(getQualifiedName() + "Was expecting updateTopo to be true but it was false");
        }
        topoUpdateStageLock.notifyAll();
      }
      break;
    case ParentAdded:
    case ChildAdded:
    case ParentRemoved:
    case ChildRemoved:
      processNeighborAcks(gcm, msgType, msgAcked, sourceId);
      break;

    default:
      LOG.fine(getQualifiedName() + "Non ACK msg " + gcm.getType() + " for " + gcm.getDestid() + " unexpected");
      break;
    }
    LOG.exiting("TaskNodeStatusImpl", "processMsg", getQualifiedName());
  }

  private void processNeighborAcks(final GroupCommunicationMessage gcm, final Type msgType, final Type msgAcked,
                                   final String sourceId) {
    LOG.entering("TaskNodeStatusImpl", "processNeighborAcks", getQualifiedName() + gcm);
    if (statusMap.containsKey(msgAcked)) {
      if (statusMap.contains(msgAcked, sourceId)) {
        statusMap.remove(msgAcked, sourceId);
        updateNeighborStatus(msgAcked, sourceId);
        checkNeighborActiveToSendTopoSetup(sourceId);
        chkIamActiveToSendTopoSetup(msgAcked);
      } else {
        LOG.fine(getQualifiedName() + "NodeStatusMsgProcessorStage Got " + msgType + " from a source(" + sourceId
            + ") to whom ChildAdd was not sent. "
            + "Perhaps reset during failure. If context not indicative use ***CAUTION***");
      }
    } else {
      LOG.fine(getQualifiedName() + "NodeStatusMsgProcessorStage There were no " + msgAcked
          + " msgs sent in the previous update cycle. "
          + "Perhaps reset during failure. If context not indicative use ***CAUTION***");
    }
    LOG.exiting("TaskNodeStatusImpl", "processNeighborAcks", getQualifiedName() + gcm);
  }

  private void checkNeighborActiveToSendTopoSetup(final String sourceId) {
    LOG.entering("TaskNodeStatusImpl", "checkNeighborActiveToSendTopoSetup", new Object[]{getQualifiedName(),
        sourceId});
    if (statusMap.notContains(sourceId)) {
      //All msgs corresponding to sourceId have been ACKed
      if (neighborStatus.get(sourceId) > 0) {
        activeNeighbors.add(sourceId);
        node.checkAndSendTopologySetupMessageFor(sourceId);
      } else {
        LOG.finest(getQualifiedName() + sourceId + " is not a neighbor anymore");
      }
    } else {
      LOG.finest(getQualifiedName() + "Not done processing " + sourceId + " acks yet. So it is still inactive");
    }
    LOG.exiting("TaskNodeStatusImpl", "checkNeighborActiveToSendTopoSetup", getQualifiedName() + sourceId);
  }

  private void updateNeighborStatus(final Type msgAcked, final String sourceId) {
    LOG.entering("TaskNodeStatusImpl", "updateNeighborStatus", new Object[]{getQualifiedName(), msgAcked, sourceId});
    if (isAddMsg(msgAcked)) {
      neighborStatus.add(sourceId);
    } else if (isDeadMsg(msgAcked)) {
      neighborStatus.remove(sourceId);
    } else {
      throw new RuntimeException("Can only deal with Neigbor ACKs while I received " + msgAcked + " from " + sourceId);
    }
    LOG.exiting("TaskNodeStatusImpl", "updateNeighborStatus", new Object[]{getQualifiedName(), msgAcked, sourceId});
  }

  @Override
  public void updatingTopology() {
    LOG.entering("TaskNodeStatusImpl", "updatingTopology", getQualifiedName());
    final boolean topoBeingUpdated = !updatingTopo.compareAndSet(false, true);
    if (topoBeingUpdated) {
      throw new RuntimeException(getQualifiedName() + "Was expecting updateTopo to be false but it was true");
    }
    LOG.exiting("TaskNodeStatusImpl", "updatingTopology", getQualifiedName());
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":(" + taskId + "," + node.getVersion() + ") - ";
  }

  @Override
  public boolean hasChanges() {
    LOG.entering("TaskNodeStatusImpl", "hasChanges", getQualifiedName());
    final boolean notEmpty = !statusMap.isEmpty();
    LOG.exiting("TaskNodeStatusImpl", "hasChanges", getQualifiedName() + notEmpty);
    return notEmpty;
  }

  @Override
  public void waitForTopologySetup() {
    LOG.entering("TaskNodeStatusImpl", "waitForTopologySetup", getQualifiedName());
    LOG.finest("Waiting to acquire topoUpdateStageLock");
    synchronized (topoUpdateStageLock) {
      LOG.finest(getQualifiedName() + "Acquired topoUpdateStageLock. updatingTopo: " + updatingTopo.get());
      while (updatingTopo.get() && node.isRunning()) {
        try {
          LOG.finest(getQualifiedName() + "Waiting on topoUpdateStageLock");
          topoUpdateStageLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException in NodeTopologyUpdateWaitStage "
              + "while waiting for receiving TopologySetup", e);
        }
      }
    }
  }
}
