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
package org.apache.reef.io.network.group.impl.task;

import org.apache.commons.lang.ArrayUtils;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.group.api.task.NodeStruct;
import org.apache.reef.io.network.group.api.task.OperatorTopologyStruct;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.operators.Sender;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 *
 */
public class OperatorTopologyStructImpl implements OperatorTopologyStruct {

  private static final int SMALL_MSG_LENGTH = 1 << 20;

  private static final Logger LOG = Logger.getLogger(OperatorTopologyStructImpl.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final String driverId;
  private final Sender sender;

  private boolean changes = true;
  private NodeStruct parent;
  private final List<NodeStruct> children = new ArrayList<>();

  private final BlockingQueue<NodeStruct> nodesWithData = new LinkedBlockingQueue<>();
  private final Set<String> childrenToRcvFrom = new HashSet<>();

  private final ConcurrentMap<String, Set<Integer>> deadMsgs = new ConcurrentHashMap<>();

  private final int version;

  public OperatorTopologyStructImpl(final Class<? extends Name<String>> groupName,
                                    final Class<? extends Name<String>> operName, final String selfId,
                                    final String driverId, final Sender sender, final int version) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfId = selfId;
    this.driverId = driverId;
    this.sender = sender;
    this.version = version;
  }

  public OperatorTopologyStructImpl(final OperatorTopologyStruct topology) {
    super();
    this.groupName = topology.getGroupName();
    this.operName = topology.getOperName();
    this.selfId = topology.getSelfId();
    this.driverId = topology.getDriverId();
    this.sender = topology.getSender();
    this.changes = topology.hasChanges();
    this.parent = topology.getParent();
    this.children.addAll(topology.getChildren());
    this.version = topology.getVersion();
  }

  @Override
  public String toString() {
    return "OperatorTopologyStruct - " + Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) +
        "(" + selfId + "," + version + ")";
  }

  @Override
  public NodeStruct getParent() {
    return parent;
  }

  @Override
  public Collection<? extends NodeStruct> getChildren() {
    return children;
  }

  @Override
  public Class<? extends Name<String>> getGroupName() {
    return groupName;
  }

  @Override
  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  @Override
  public String getSelfId() {
    return selfId;
  }

  @Override
  public String getDriverId() {
    return driverId;
  }

  @Override
  public Sender getSender() {
    return sender;
  }

  @Override
  public boolean hasChanges() {
    LOG.entering("OperatorTopologyStructImpl", "hasChanges", getQualifiedName());
    LOG.exiting("OperatorTopologyStructImpl", "hasChanges",
        Arrays.toString(new Object[]{this.changes, getQualifiedName()}));
    return this.changes;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public void addAsData(final GroupCommunicationMessage msg) {
    LOG.entering("OperatorTopologyStructImpl", "addAsData", new Object[]{getQualifiedName(), msg});
    final String srcId = msg.getSrcid();
    final NodeStruct node = findNode(srcId);
    if (node != null) {
      try {
        nodesWithData.put(node);
        LOG.finest(getQualifiedName() + "Added node " + srcId + " to nodesWithData queue");
      } catch (final InterruptedException e) {
        throw new RuntimeException("InterruptedException while adding to childrenWithData queue", e);
      }
      node.addData(msg);
    } else {
      LOG.fine("Unable to find node " + srcId + " to send " + msg.getType() + " to");
    }
    LOG.exiting("OperatorTopologyStructImpl", "addAsData", Arrays.toString(new Object[]{getQualifiedName(), msg}));
  }

  private NodeStruct findNode(final String srcId) {
    LOG.entering("OperatorTopologyStructImpl", "findNode", new Object[]{getQualifiedName(), srcId});
    final NodeStruct retVal;
    if (parent != null && parent.getId().equals(srcId)) {
      retVal = parent;
    } else {
      retVal = findChild(srcId);
    }
    LOG.exiting("OperatorTopologyStructImpl", "findNode",
        Arrays.toString(new Object[]{retVal, getQualifiedName(), srcId}));
    return retVal;
  }

  private void sendToNode(final byte[] data,
                          final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType,
                          final NodeStruct node) {
    LOG.entering("OperatorTopologyStructImpl", "sendToNode", new Object[]{getQualifiedName(), msgType, node});
    final String nodeId = node.getId();
    try {

      if (data.length > SMALL_MSG_LENGTH) {
        LOG.finest(getQualifiedName() + "Msg too big. Sending readiness to send " + msgType + " msg to " + nodeId);
        sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfId, version, nodeId, node.getVersion(),
            Utils.EMPTY_BYTE_ARR));
        final byte[] tmpVal = receiveFromNode(node, true);
        if (tmpVal != null) {
          LOG.finest(getQualifiedName() + "Got readiness to accept " + msgType + " msg from " + nodeId
              + ". Will send actual msg now");
        } else {
          LOG.exiting("OperatorTopologyStructImpl", "sendToNode", getQualifiedName());
          return;
        }
      }

      sender.send(Utils.bldVersionedGCM(groupName, operName, msgType, selfId, version, nodeId, node.getVersion(),
          data));

      if (data.length > SMALL_MSG_LENGTH) {
        LOG.finest(getQualifiedName() + "Msg too big. Will wait for ACK before queing up one more msg");
        final byte[] tmpVal = receiveFromNode(node, true);
        if (tmpVal != null) {
          LOG.finest(getQualifiedName() + "Got " + msgType + " msg received ACK from " + nodeId
              + ". Will move to next msg if it exists");
        } else {
          LOG.exiting("OperatorTopologyStructImpl", "sendToNode", getQualifiedName());
          return;
        }
      }
    } catch (final NetworkException e) {
      throw new RuntimeException(
          "NetworkException while sending " + msgType + " data from " + selfId + " to " + nodeId,
          e);
    }
    LOG.exiting("OperatorTopologyStructImpl", "sendToNode", getQualifiedName());
  }

  private byte[] receiveFromNode(final NodeStruct node, final boolean remove) {
    LOG.entering("OperatorTopologyStructImpl", "receiveFromNode", new Object[]{getQualifiedName(), node, remove});
    final byte[] retVal = node.getData();
    if (remove) {
      final boolean removed = nodesWithData.remove(node);
      final String msg = getQualifiedName() + "Removed(" + removed + ") node " + node.getId()
          + " from nodesWithData queue";
      if (removed) {
        LOG.finest(msg);
      } else {
        LOG.fine(msg);
      }
    }
    LOG.exiting("OperatorTopologyStructImpl", "receiveFromNode", getQualifiedName());
    return retVal;
  }

  /**
   * Receive data from {@code node}, while checking if it is trying to send a big message.
   * Nodes that send big messages will first send an empty data message and
   * wait for an ACK before transmitting the actual big message. Thus the
   * receiving side checks whether a message is empty or not, and after sending
   * an ACK it must wait for another message if the first message was empty.
   *
   * @param node node to receive a message from
   * @param msgType message type
   * @return message sent from {@code node}
   */
  private byte[] recvFromNodeCheckBigMsg(final NodeStruct node,
                                         final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) {
    LOG.entering("OperatorTopologyStructImpl", "recvFromNodeCheckBigMsg", new Object[]{node, msgType});

    byte[] retVal = receiveFromNode(node, false);
    if (retVal != null && retVal.length == 0) {
      LOG.finest(getQualifiedName() + " Got msg that node " + node.getId()
          + " has large data and is ready to send it. Sending ACK to receive data.");
      sendToNode(Utils.EMPTY_BYTE_ARR, msgType, node);
      retVal = receiveFromNode(node, true);

      if (retVal != null) {
        LOG.finest(getQualifiedName() + " Received large msg from node " + node.getId()
            + ". Will process it after ACKing.");
        sendToNode(Utils.EMPTY_BYTE_ARR, msgType, node);
      } else {
        LOG.warning(getQualifiedName() + "Expected large msg from node " + node.getId()
            + " but received nothing.");
      }
    }

    LOG.exiting("OperatorTopologyStructImpl", "recvFromNodeCheckBigMsg");
    return retVal;
  }

  /**
   * Retrieves and removes the head of {@code nodesWithData}, waiting if necessary until an element becomes available.
   * (Comment taken from {@link java.util.concurrent.BlockingQueue})
   * If interrupted while waiting, then throws a RuntimeException.
   *
   * @return the head of this queue
   */
  private NodeStruct nodesWithDataTakeUnsafe() {
    LOG.entering("OperatorTopologyStructImpl", "nodesWithDataTakeUnsafe");
    try {
      final NodeStruct child = nodesWithData.take();
      LOG.exiting("OperatorTopologyStructImpl", "nodesWithDataTakeUnsafe", child);
      return child;

    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting to take data from nodesWithData queue", e);
    }
  }

  @Override
  public void sendToParent(final byte[] data, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) {
    LOG.entering("OperatorTopologyStructImpl", "sendToParent", new Object[]{getQualifiedName(), msgType});
    if (parent != null) {
      sendToNode(data, msgType, parent);
    } else {
      LOG.fine(getQualifiedName() + "Perhaps parent has died or has not been configured");
    }
    LOG.exiting("OperatorTopologyStructImpl", "sendToParent", getQualifiedName());
  }

  @Override
  public void sendToChildren(final byte[] data, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) {
    LOG.entering("OperatorTopologyStructImpl", "sendToChildren", new Object[]{getQualifiedName(), msgType});
    for (final NodeStruct child : children) {
      sendToNode(data, msgType, child);
    }
    LOG.exiting("OperatorTopologyStructImpl", "sendToChildren", getQualifiedName());
  }

  @Override
  public void sendToChildren(final Map<String, byte[]> dataMap,
                             final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) {
    LOG.entering("OperatorTopologyStructImpl", "sendToChildren", new Object[]{getQualifiedName(), msgType});
    for (final NodeStruct child : children) {
      if (dataMap.containsKey(child.getId())) {
        sendToNode(dataMap.get(child.getId()), msgType, child);
      } else {
        throw new RuntimeException("No message specified for " + child.getId() + " in dataMap.");
      }
    }
    LOG.exiting("OperatorTopologyStructImpl", "sendToChildren", getQualifiedName());
  }

  @Override
  public byte[] recvFromParent(final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType) {
    LOG.entering("OperatorTopologyStructImpl", "recvFromParent", getQualifiedName());
    LOG.finest(getQualifiedName() + "Waiting for " + parent.getId() + " to send data");
    final byte[] retVal = recvFromNodeCheckBigMsg(parent, msgType);
    LOG.exiting("OperatorTopologyStructImpl", "recvFromParent", getQualifiedName());
    return retVal;
  }

  @Override
  public <T> T recvFromChildren(final ReduceFunction<T> redFunc, final Codec<T> dataCodec) {
    LOG.entering("OperatorTopologyStructImpl", "recvFromChildren", new Object[]{getQualifiedName(), redFunc,
        dataCodec});
    final List<T> retLst = new ArrayList<>(2);
    for (final NodeStruct child : children) {
      childrenToRcvFrom.add(child.getId());
    }

    while (!childrenToRcvFrom.isEmpty()) {
      LOG.finest(getQualifiedName() + "Waiting for some child to send data");
      final NodeStruct child = nodesWithDataTakeUnsafe();
      final byte[] retVal = recvFromNodeCheckBigMsg(child,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.Reduce);

      if (retVal != null) {
        retLst.add(dataCodec.decode(retVal));
        if (retLst.size() == 2) {
          final T redVal = redFunc.apply(retLst);
          retLst.clear();
          retLst.add(redVal);
        }
      }
      childrenToRcvFrom.remove(child.getId());
    }
    final T retVal = retLst.isEmpty() ? null : retLst.get(0);
    LOG.exiting("OperatorTopologyStructImpl", "recvFromChildren", getQualifiedName());
    return retVal;
  }

  /**
   * Receive data from all children as a single byte array.
   * Messages from children are simply byte-concatenated.
   * This method is currently used only by the Gather operator.
   *
   * @return gathered data as a byte array
   */
  @Override
  public byte[] recvFromChildren() {
    LOG.entering("OperatorTopologyStructImpl", "recvFromChildren", getQualifiedName());
    for (final NodeStruct child : children) {
      childrenToRcvFrom.add(child.getId());
    }

    byte[] retVal = new byte[0];
    while (!childrenToRcvFrom.isEmpty()) {
      LOG.finest(getQualifiedName() + "Waiting for some child to send data");
      final NodeStruct child = nodesWithDataTakeUnsafe();
      final byte[] receivedVal = recvFromNodeCheckBigMsg(child,
          ReefNetworkGroupCommProtos.GroupCommMessage.Type.Gather);

      if (receivedVal != null) {
        retVal = ArrayUtils.addAll(retVal, receivedVal);
      }
      childrenToRcvFrom.remove(child.getId());
    }

    LOG.exiting("OperatorTopologyStructImpl", "recvFromChildren", getQualifiedName());
    return retVal;
  }

  private boolean removedDeadMsg(final String msgSrcId, final int msgSrcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "removedDeadMsg", new Object[]{getQualifiedName(), msgSrcId,
        msgSrcVersion});
    boolean retVal = false;
    final Set<Integer> msgVersions = deadMsgs.get(msgSrcId);
    if (msgVersions != null) {
      LOG.fine(getQualifiedName() + "Found dead msgs " + msgVersions + " waiting for add");
      if (msgVersions.remove(msgSrcVersion)) {
        LOG.fine(getQualifiedName() + "Found dead msg with same version as srcVer-" + msgSrcVersion);
        retVal = true;
      } else {
        LOG.finest(getQualifiedName() + "No dead msg with same version as srcVer-" + msgSrcVersion);
      }
    } else {
      LOG.finest(getQualifiedName() + "No dead msgs waiting for add.");
    }
    LOG.exiting("OperatorTopologyStructImpl", "removedDeadMsg",
        new Object[]{retVal, getQualifiedName(), msgSrcId, msgSrcVersion});
    return retVal;
  }

  private void addToDeadMsgs(final String srcId, final int srcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "addToDeadMsgs", new Object[]{getQualifiedName(), srcId, srcVersion});
    deadMsgs.putIfAbsent(srcId, new HashSet<Integer>());
    deadMsgs.get(srcId).add(srcVersion);
    LOG.exiting("OperatorTopologyStructImpl", "addToDeadMsgs", Arrays.toString(new Object[]{getQualifiedName(),
        srcId, srcVersion}));
  }

  private boolean addedToDeadMsgs(final NodeStruct node, final String msgSrcId, final int msgSrcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "addedToDeadMsgs", new Object[]{getQualifiedName(), node, msgSrcId,
        msgSrcVersion});
    if (node == null) {
      LOG.warning(getQualifiedName() + "Got dead msg when no node existed. OOS Queuing up for add to handle");
      addToDeadMsgs(msgSrcId, msgSrcVersion);
      LOG.exiting("OperatorTopologyStructImpl", "addedToDeadMsgs",
          Arrays.toString(new Object[]{true, getQualifiedName(), null, msgSrcId, msgSrcVersion}));
      return true;
    }
    final int nodeVersion = node.getVersion();
    if (msgSrcVersion > nodeVersion) {
      LOG.warning(getQualifiedName() + "Got an OOS dead msg. " + "Has HIGHER ver-" + msgSrcVersion + " than node ver-"
          + nodeVersion + ". Queing up for add to handle");
      addToDeadMsgs(msgSrcId, msgSrcVersion);
      LOG.exiting("OperatorTopologyStructImpl", "addedToDeadMsgs",
          Arrays.toString(new Object[]{true, getQualifiedName(), node, msgSrcId, msgSrcVersion}));
      return true;
    }
    LOG.exiting("OperatorTopologyStructImpl", "addedToDeadMsgs",
        Arrays.toString(new Object[]{false, getQualifiedName(), node, msgSrcId, msgSrcVersion}));
    return false;
  }

  /**
   * Updates the topology structure with the received
   * message. Does not make assumptions about msg order
   * Tries to handle OOS msgs
   * <p>
   * Expects only control messages
   */
  @Override
  public void update(final GroupCommunicationMessage msg) {
    if (msg.hasSrcVersion()) {
      final String srcId = msg.getSrcid();
      final int srcVersion = msg.getSrcVersion();
      LOG.finest(getQualifiedName() + "Updating " + msg.getType() + " msg from " + srcId);
      LOG.finest(getQualifiedName() + "Before update: parent=" + ((parent != null) ? parent.getId() : "NULL"));
      LOG.finest(getQualifiedName() + "Before update: children=" + children);
      switch (msg.getType()) {
      case ParentAdd:
        updateParentAdd(srcId, srcVersion);
        break;
      case ParentDead:
        updateParentDead(srcId, srcVersion);
        break;
      case ChildAdd:
        updateChildAdd(srcId, srcVersion);
        break;
      case ChildDead:
        updateChildDead(srcId, srcVersion);
        break;
      default:
        throw new RuntimeException("Received a non control message in update");
      }
      LOG.finest(getQualifiedName() + "After update: parent=" + ((parent != null) ? parent.getId() : "NULL"));
      LOG.finest(getQualifiedName() + "After update: children=" + children);
    } else {
      throw new RuntimeException(getQualifiedName() + "can only deal with msgs that have src version set");
    }
  }

  private void updateChildDead(final String srcId, final int srcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "updateChildDead",
        new Object[]{getQualifiedName(), srcId, srcVersion});
    final NodeStruct toBeRemovedchild = findChild(srcId);
    if (!addedToDeadMsgs(toBeRemovedchild, srcId, srcVersion)) {
      final int childVersion = toBeRemovedchild.getVersion();
      if (srcVersion < childVersion) {
        LOG.finest(getQualifiedName() + "Got an OOS child dead msg. " + "Has LOWER ver-" + srcVersion
            + " than child ver-" + childVersion + ". Discarding");
        LOG.exiting("OperatorTopologyStructImpl", "updateChildDead", Arrays.toString(new Object[]{getQualifiedName(),
            srcId, srcVersion}));
        return;
      } else {
        LOG.finest(getQualifiedName() + "Got a child dead msg. " + "Has SAME ver-" + srcVersion + " as child ver-"
            + childVersion + "Removing child node");
      }
    } else {
      LOG.fine(getQualifiedName() + "Added to dead msgs. Removing child node since ChildAdd might not turn up");
    }
    children.remove(toBeRemovedchild);
    LOG.exiting("OperatorTopologyStructImpl", "updateChildDead", Arrays.toString(new Object[]{getQualifiedName(),
        srcId, srcVersion}));
  }

  private void updateChildAdd(final String srcId, final int srcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "updateChildAdd", new Object[]{getQualifiedName(), srcId, srcVersion});
    if (!removedDeadMsg(srcId, srcVersion)) {
      final NodeStruct toBeAddedchild = findChild(srcId);
      if (toBeAddedchild != null) {
        LOG.warning(getQualifiedName() + "Child already exists");
        final int childVersion = toBeAddedchild.getVersion();
        if (srcVersion < childVersion) {
          LOG.fine(getQualifiedName() + "Got an OOS child add msg. " + "Has LOWER ver-" + srcVersion
              + " than child ver-" + childVersion + ". Discarding");
          LOG.exiting("OperatorTopologyStructImpl", "updateChildAdd",
              Arrays.toString(new Object[]{getQualifiedName(), srcId, srcVersion}));
          return;
        }
        if (srcVersion > childVersion) {
          LOG.fine(getQualifiedName() + "Got an OOS child add msg. " + "Has HIGHER ver-" + srcVersion
              + " than child ver-" + childVersion + ". Bumping up version number");
          toBeAddedchild.setVersion(srcVersion);
          LOG.exiting("OperatorTopologyStructImpl", "updateChildAdd",
              Arrays.toString(new Object[]{getQualifiedName(), srcId, srcVersion}));
          return;
        } else {
          throw new RuntimeException(getQualifiedName() + "Got two child add msgs of same version-" + srcVersion);
        }
      } else {
        LOG.finest(getQualifiedName() + "Creating new child node for " + srcId);
        children.add(new ChildNodeStruct(srcId, srcVersion));
      }
    } else {
      LOG.warning(getQualifiedName() + "Removed dead msg. Not adding child");
    }
    LOG.exiting("OperatorTopologyStructImpl", "updateChildAdd", Arrays.toString(new Object[]{getQualifiedName(),
        srcId, srcVersion}));
  }

  private void updateParentDead(final String srcId, final int srcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "updateParentDead",
        new Object[]{getQualifiedName(), srcId, srcVersion});
    if (!addedToDeadMsgs(parent, srcId, srcVersion)) {
      final int parentVersion = parent.getVersion();
      if (srcVersion < parentVersion) {
        LOG.fine(getQualifiedName() + "Got an OOS parent dead msg. " + "Has LOWER ver-" + srcVersion
            + " than parent ver-" + parentVersion + ". Discarding");
        LOG.exiting("OperatorTopologyStructImpl", "updateParentDead",
            Arrays.toString(new Object[]{getQualifiedName(), srcId, srcVersion}));
        return;
      } else {
        LOG.finest(getQualifiedName() + "Got a parent dead msg. " + "Has SAME ver-" + srcVersion + " as parent ver-"
            + parentVersion + "Setting parent node to null");
      }
    } else {
      LOG.warning(getQualifiedName() + "Added to dead msgs. Setting parent to null since ParentAdd might not turn up");
    }
    parent = null;
    LOG.exiting("OperatorTopologyStructImpl", "updateParentDead", Arrays.toString(new Object[]{getQualifiedName(),
        srcId, srcVersion}));
  }

  private void updateParentAdd(final String srcId, final int srcVersion) {
    LOG.entering("OperatorTopologyStructImpl", "updateParentAdd",
        new Object[]{getQualifiedName(), srcId, srcVersion});
    if (!removedDeadMsg(srcId, srcVersion)) {
      if (parent != null) {
        LOG.fine(getQualifiedName() + "Parent already exists");
        final int parentVersion = parent.getVersion();
        if (srcVersion < parentVersion) {
          LOG.fine(getQualifiedName() + "Got an OOS parent add msg. " + "Has LOWER ver-" + srcVersion
              + " than parent ver-" + parentVersion + ". Discarding");
          LOG.exiting("OperatorTopologyStructImpl", "updateParentAdd",
              Arrays.toString(new Object[]{getQualifiedName(), srcId, srcVersion}));
          return;
        }
        if (srcVersion > parentVersion) {
          LOG.fine(getQualifiedName() + "Got an OOS parent add msg. " + "Has HIGHER ver-" + srcVersion
              + " than parent ver-" + parentVersion + ". Bumping up version number");
          parent.setVersion(srcVersion);
          LOG.exiting("OperatorTopologyStructImpl", "updateParentAdd",
              Arrays.toString(new Object[]{getQualifiedName(), srcId, srcVersion}));
          return;
        } else {
          throw new RuntimeException(getQualifiedName() + "Got two parent add msgs of same version-" + srcVersion);
        }
      } else {
        LOG.finest(getQualifiedName() + "Creating new parent node for " + srcId);
        parent = new ParentNodeStruct(srcId, srcVersion);
      }
    } else {
      LOG.fine(getQualifiedName() + "Removed dead msg. Not adding parent");
    }
    LOG.exiting("OperatorTopologyStructImpl", "updateParentAdd", Arrays.toString(new Object[]{getQualifiedName(),
        srcId, srcVersion}));
  }

  /**
   * @param srcId
   * @return
   */
  private NodeStruct findChild(final String srcId) {
    LOG.entering("OperatorTopologyStructImpl", "findChild", new Object[]{getQualifiedName(), srcId});
    NodeStruct retVal = null;
    for (final NodeStruct node : children) {
      if (node.getId().equals(srcId)) {
        retVal = node;
        break;
      }
    }
    LOG.exiting("OperatorTopologyStructImpl", "findChild", Arrays.toString(new Object[]{retVal, getQualifiedName(),
        srcId}));
    return retVal;
  }

  @Override
  public void update(final Set<GroupCommunicationMessage> deletionDeltas) {
    LOG.entering("OperatorTopologyStructImpl", "update", new Object[]{"Updating topology with deleting msgs",
        getQualifiedName(), deletionDeltas});
    for (final GroupCommunicationMessage delDelta : deletionDeltas) {
      update(delDelta);
    }
    LOG.exiting("OperatorTopologyStructImpl", "update", Arrays.toString(new Object[]{getQualifiedName(),
        deletionDeltas}));
  }

  @Override
  public void setChanges(final boolean changes) {
    LOG.entering("OperatorTopologyStructImpl", "setChanges", new Object[]{getQualifiedName(), changes});
    this.changes = changes;
    LOG.exiting("OperatorTopologyStructImpl", "setChanges",
        Arrays.toString(new Object[]{getQualifiedName(), changes}));
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":" + selfId + ":ver(" + version + ") - ";
  }
}
