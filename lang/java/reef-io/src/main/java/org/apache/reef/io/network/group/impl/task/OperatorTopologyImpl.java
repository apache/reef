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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.exception.ParentDeadException;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.task.OperatorTopology;
import org.apache.reef.io.network.group.api.task.OperatorTopologyStruct;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.operators.Sender;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class OperatorTopologyImpl implements OperatorTopology {

  private static final Logger LOG = Logger.getLogger(OperatorTopologyImpl.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final String driverId;
  private final Sender sender;
  private final Object topologyLock = new Object();

  private final int version;

  private final BlockingQueue<GroupCommunicationMessage> deltas = new LinkedBlockingQueue<>();
  private final BlockingQueue<GroupCommunicationMessage> deletionDeltas = new LinkedBlockingQueue<>();

  private OperatorTopologyStruct baseTopology;
  private OperatorTopologyStruct effectiveTopology;
  private final ResettingCountDownLatch topologyLockAcquired = new ResettingCountDownLatch(1);
  private final AtomicBoolean updatingTopo = new AtomicBoolean(false);

  private final EventHandler<GroupCommunicationMessage> baseTopologyUpdateHandler = new BaseTopologyUpdateHandler();

  private final EStage<GroupCommunicationMessage> baseTopologyUpdateStage = new SingleThreadStage<>(
      "BaseTopologyUpdateStage",
      baseTopologyUpdateHandler,
      5);

  private final EventHandler<GroupCommunicationMessage> dataHandlingStageHandler = new DataHandlingStageHandler();

  // The queue capacity might determine how many tasks can be handled
  private final EStage<GroupCommunicationMessage> dataHandlingStage = new SingleThreadStage<>("DataHandlingStage",
      dataHandlingStageHandler,
      10000);

  public OperatorTopologyImpl(final Class<? extends Name<String>> groupName,
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

  /**
   * Handle messages meant for this operator. Data msgs are passed on
   * to the DataHandlingStage while Ctrl msgs are queued up for the
   * base topology to update later. Ctrl msgs signalling death of a
   * task are also routed to the effectiveTopology in order to notify
   * a waiting operation. During initialization when effective topology
   * is not yet set-up, these *Dead msgs are queued in deletionDeltas
   * for the small time window when these arrive after baseTopology has
   * received TopologySetup but not yet created the effectiveTopology.
   * Most times the msgs in the deletionDeltas will be discarded as stale
   * msgs
   * <p>
   * No synchronization is needed while handling *Dead messages.
   * There 2 states: UpdatingTopo and NotUpdatingTopo
   * If UpdatingTopo, deltas.put still takes care of adding this msg to effTop through baseTopo changes.
   * If not, we add to effTopo. So we are good.
   * <p>
   * However, for data msgs synchronization is needed. Look at doc of
   * DataHandlingStage
   * <p>
   * Adding to deletionDeltas should be outside
   * effTopo!=null block. There is a rare possibility that during initialization
   * just after baseTopo is created(so deltas will be ignored) and just before
   * effTopo is created(so effTopo will be null) where we can miss a deletion
   * msg if not added to deletionDelta because this method is synchronized
   */
  @Override
  public void handle(final GroupCommunicationMessage msg) {
    LOG.entering("OperatorTopologyImpl", "handle", new Object[]{getQualifiedName(), msg});
    if (isMsgVersionOk(msg)) {
      try {
        switch (msg.getType()) {
        case UpdateTopology:
          updatingTopo.set(true);
          baseTopologyUpdateStage.onNext(msg);
          topologyLockAcquired.awaitAndReset(1);
          LOG.finest(getQualifiedName() + "topoLockAcquired CDL released. Resetting it to new CDL");
          sendAckToDriver(msg);
          break;

        case TopologySetup:
          LOG.finest(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);
          break;

        case ParentAdd:
        case ChildAdd:
          LOG.finest(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);
          break;

        case ParentDead:
        case ChildDead:
          LOG.finest(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);

          LOG.finest(getQualifiedName() + "Adding to deletionDeltas queue");
          deletionDeltas.put(msg);

          if (effectiveTopology != null) {
            LOG.finest(getQualifiedName() + "Adding as data msg to non-null effective topology struct");
            effectiveTopology.addAsData(msg);
          } else {
            LOG.fine(getQualifiedName() + "Received a death message before effective topology was setup. CAUTION");
          }
          break;

        default:
          dataHandlingStage.onNext(msg);
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException("InterruptedException while trying to put ctrl msg into delta queue", e);
      }
    }
    LOG.exiting("OperatorTopologyImpl", "handle", Arrays.toString(new Object[]{getQualifiedName(), msg}));
  }

  private boolean isMsgVersionOk(final GroupCommunicationMessage msg) {
    LOG.entering("OperatorTopologyImpl", "isMsgVersionOk", new Object[]{getQualifiedName(), msg});
    if (msg.hasVersion()) {
      final int msgVersion = msg.getVersion();
      final boolean retVal;
      if (msgVersion < version) {
        LOG.warning(getQualifiedName() + "Received a ver-" + msgVersion + " msg while expecting ver-" + version
            + ". Discarding msg");
        retVal = false;
      } else {
        retVal = true;
      }
      LOG.exiting("OperatorTopologyImpl", "isMsgVersionOk",
          Arrays.toString(new Object[]{retVal, getQualifiedName(), msg}));
      return retVal;
    } else {
      throw new RuntimeException(getQualifiedName() + "can only deal with versioned msgs");
    }
  }

  @Override
  public void initialize() throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "initialize", getQualifiedName());
    createBaseTopology();
    LOG.exiting("OperatorTopologyImpl", "initialize", getQualifiedName());
  }

  @Override
  public void sendToParent(final byte[] data, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "sendToParent", new Object[] {getQualifiedName(), msgType});
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    effectiveTopology.sendToParent(data, msgType);
    LOG.exiting("OperatorTopologyImpl", "sendToParent", getQualifiedName());
  }

  @Override
  public void sendToChildren(final byte[] data, final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "sendToChildren", new Object[]{getQualifiedName(), msgType});
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    effectiveTopology.sendToChildren(data, msgType);
    LOG.exiting("OperatorTopologyImpl", "sendToChildren", getQualifiedName());
  }

  @Override
  public void sendToChildren(final Map<String, byte[]> dataMap,
                             final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "sendToChildren", new Object[]{getQualifiedName(), msgType});
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    effectiveTopology.sendToChildren(dataMap, msgType);
    LOG.exiting("OperatorTopologyImpl", "sendToChildren", getQualifiedName());
  }

  @Override
  public byte[] recvFromParent(final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "recvFromParent", new Object[] {getQualifiedName(), msgType});
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    final byte[] retVal = effectiveTopology.recvFromParent(msgType);
    LOG.exiting("OperatorTopologyImpl", "recvFromParent", getQualifiedName());
    return retVal;
  }

  @Override
  public <T> T recvFromChildren(final Reduce.ReduceFunction<T> redFunc, final Codec<T> dataCodec)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "recvFromChildren", getQualifiedName());
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    final T retVal = effectiveTopology.recvFromChildren(redFunc, dataCodec);
    LOG.exiting("OperatorTopologyImpl", "recvFromChildren", getQualifiedName());
    return retVal;
  }

  @Override
  public byte[] recvFromChildren() throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "recvFromChildren", getQualifiedName());
    refreshEffectiveTopology();
    assert effectiveTopology != null;
    final byte[] retVal = effectiveTopology.recvFromChildren();
    LOG.exiting("OperatorTopologyImpl", "recvFromChildren", getQualifiedName());
    return retVal;
  }

  /**
   * Only refreshes the effective topology with deletion msgs from.
   * deletionDeltas queue
   *
   * @throws ParentDeadException
   */
  private void refreshEffectiveTopology() throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "refreshEffectiveTopology", getQualifiedName());
    LOG.finest(getQualifiedName() + "Waiting to acquire topoLock");
    synchronized (topologyLock) {
      LOG.finest(getQualifiedName() + "Acquired topoLock");

      assert effectiveTopology != null;

      final Set<GroupCommunicationMessage> deletionDeltasSet = new HashSet<>();
      copyDeletionDeltas(deletionDeltasSet);

      LOG.finest(getQualifiedName() + "Updating effective topology struct with deletion msgs");
      effectiveTopology.update(deletionDeltasSet);
      LOG.finest(getQualifiedName() + "Released topoLock");
    }
    LOG.exiting("OperatorTopologyImpl", "refreshEffectiveTopology", getQualifiedName());
  }

  /**
   * @throws ParentDeadException
   */
  private void createBaseTopology() throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "createBaseTopology", getQualifiedName());
    baseTopology = new OperatorTopologyStructImpl(groupName, operName, selfId, driverId, sender, version);
    updateBaseTopology();
    LOG.exiting("OperatorTopologyImpl", "createBaseTopology", getQualifiedName());
  }

  /**
   * Blocking method that waits till the base topology is updated Unblocks when.
   * we receive a TopologySetup msg from driver
   * <p>
   * Will also update the effective topology when the base topology is updated
   * so that creation of effective topology is limited to just this method and
   * refresh will only refresh the effective topology with deletion msgs from
   * deletionDeltas queue
   *
   * @throws ParentDeadException
   */
  private void updateBaseTopology() throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "updateBaseTopology", getQualifiedName());
    LOG.finest(getQualifiedName() + "Waiting to acquire topoLock");
    synchronized (topologyLock) {
      LOG.finest(getQualifiedName() + "Acquired topoLock");
      try {
        assert baseTopology != null;
        LOG.finest(getQualifiedName() + "Updating base topology. So setting dirty bit");
        baseTopology.setChanges(true);

        LOG.finest(getQualifiedName() + "Waiting for ctrl msgs");
        for (GroupCommunicationMessage msg = deltas.take();
             msg.getType() != ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologySetup;
             msg = deltas.take()) {
          LOG.finest(getQualifiedName() + "Got " + msg.getType() + " msg from " + msg.getSrcid());
          if (effectiveTopology == null &&
              msg.getType() == ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentDead) {
            /**
             * If effectiveTopology!=null, this method is being called from the BaseTopologyUpdateStage
             * And exception thrown will be caught by uncaughtExceptionHandler leading to System.exit
             */
            LOG.finer(getQualifiedName() + "Throwing ParentDeadException");
            throw new ParentDeadException(getQualifiedName()
                + "Parent dead. Current behavior is for the child to die too.");
          } else {
            LOG.finest(getQualifiedName() + "Updating baseTopology struct");
            baseTopology.update(msg);
            sendAckToDriver(msg);
          }
          LOG.finest(getQualifiedName() + "Waiting for ctrl msgs");
        }

        updateEffTopologyFromBaseTopology();

      } catch (final InterruptedException e) {
        throw new RuntimeException("InterruptedException while waiting for delta msg from driver", e);
      }
      LOG.finest(getQualifiedName() + "Released topoLock");
    }
    LOG.exiting("OperatorTopologyImpl", "updateBaseTopology", getQualifiedName());
  }

  private void sendAckToDriver(final GroupCommunicationMessage msg) {
    LOG.entering("OperatorTopologyImpl", "sendAckToDriver", new Object[]{getQualifiedName(), msg});
    try {
      final String srcId = msg.getSrcid();
      if (msg.hasVersion()) {
        final int srcVersion = msg.getSrcVersion();
        switch (msg.getType()) {
        case UpdateTopology:
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologySetup, selfId, this.version, driverId,
                srcVersion, Utils.EMPTY_BYTE_ARR));
          break;
        case ParentAdd:
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentAdded, selfId, this.version, srcId,
                srcVersion, Utils.EMPTY_BYTE_ARR), driverId);
          break;
        case ParentDead:
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentRemoved, selfId, this.version, srcId,
                srcVersion, Utils.EMPTY_BYTE_ARR), driverId);
          break;
        case ChildAdd:
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildAdded, selfId, this.version, srcId,
                srcVersion, Utils.EMPTY_BYTE_ARR), driverId);
          break;
        case ChildDead:
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildRemoved, selfId, this.version, srcId,
                srcVersion, Utils.EMPTY_BYTE_ARR), driverId);
          break;
        default:
          throw new RuntimeException("Received a non control message for acknowledgement");
        }
      } else {
        throw new RuntimeException(getQualifiedName() + "Ack Sender can only deal with versioned msgs");
      }
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending ack to driver for delta msg " + msg.getType(), e);
    }
    LOG.exiting("OperatorTopologyImpl", "sendAckToDriver", Arrays.toString(new Object[]{getQualifiedName(), msg}));
  }

  private void updateEffTopologyFromBaseTopology() {
    LOG.entering("OperatorTopologyImpl", "updateEffTopologyFromBaseTopology", getQualifiedName());
    assert baseTopology != null;
    LOG.finest(getQualifiedName() + "Updating effective topology");
    if (baseTopology.hasChanges()) {
      //Create effectiveTopology from baseTopology
      effectiveTopology = new OperatorTopologyStructImpl(baseTopology);
      baseTopology.setChanges(false);
    }
    LOG.exiting("OperatorTopologyImpl", "updateEffTopologyFromBaseTopology", getQualifiedName());
  }

  /**
   * @param deletionDeltasForUpdate
   * @throws ParentDeadException
   */
  private void copyDeletionDeltas(final Set<GroupCommunicationMessage> deletionDeltasForUpdate)
      throws ParentDeadException {
    LOG.entering("OperatorTopologyImpl", "copyDeletionDeltas", getQualifiedName());
    this.deletionDeltas.drainTo(deletionDeltasForUpdate);
    for (final GroupCommunicationMessage msg : deletionDeltasForUpdate) {
      final ReefNetworkGroupCommProtos.GroupCommMessage.Type msgType = msg.getType();
      if (msgType == ReefNetworkGroupCommProtos.GroupCommMessage.Type.ParentDead) {
        throw new ParentDeadException(getQualifiedName() +
            "Parent dead. Current behavior is for the child to die too.");
      }
    }
    LOG.exiting("OperatorTopologyImpl", "copyDeletionDeltas", getQualifiedName());
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":" + selfId + ":ver(" + version + ") - ";
  }

  /**
   * Unlike Dead msgs this needs to be synchronized because data msgs are not
   * routed through the base topo changes So we need to make sure to wait for
   * updateTopo to complete and for the new effective topo to take effect. Hence
   * updatingTopo is set to false in refreshEffTopo. Also, since this is called
   * from a netty IO thread, we need to create a stage to move the msgs from
   * netty space to application space and release the netty threads. Otherwise
   * weird deadlocks can happen Ex: Sent model to k nodes using broadcast. Send
   * to K+1 th is waiting for ACK. The K nodes already compute their states and
   * reduce send their results. If we haven't finished refreshEffTopo because of
   * which updatingTopo is true, we can't add the new msgs if the #netty threads
   * is k All k threads are waiting to add data. Single user thread that is
   * waiting for ACK does not come around to refreshEffTopo and we are
   * deadlocked because there aren't enough netty threads to dispatch msgs to
   * the application. Hence the stage
   */
  private final class DataHandlingStageHandler implements EventHandler<GroupCommunicationMessage> {
    @Override
    public void onNext(final GroupCommunicationMessage dataMsg) {
      LOG.entering("OperatorTopologyImpl.DataHandlingStageHandler", "onNext", new Object[]{getQualifiedName(),
          dataMsg});
      LOG.finest(getQualifiedName() + "Waiting to acquire topoLock");
      synchronized (topologyLock) {
        LOG.finest(getQualifiedName() + "Acquired topoLock");
        while (updatingTopo.get()) {
          try {
            LOG.finest(getQualifiedName() + "Topology is being updated. Released topoLock, Waiting on topoLock");
            topologyLock.wait();
            LOG.finest(getQualifiedName() + "Acquired topoLock");
          } catch (final InterruptedException e) {
            throw new RuntimeException("InterruptedException while data handling"
                + "stage was waiting for updatingTopo to become false", e);
          }
        }
        if (effectiveTopology != null) {
          LOG.finest(getQualifiedName() + "Non-null effectiveTopo.addAsData(msg)");
          effectiveTopology.addAsData(dataMsg);
        } else {
          LOG.fine("Received a data message before effective topology was setup");
        }
        LOG.finest(getQualifiedName() + "Released topoLock");
      }
      LOG.exiting("OperatorTopologyImpl.DataHandlingStageHandler", "onNext",
          Arrays.toString(new Object[]{getQualifiedName(), dataMsg}));
    }
  }

  private final class BaseTopologyUpdateHandler implements EventHandler<GroupCommunicationMessage> {
    @Override
    public void onNext(final GroupCommunicationMessage msg) {
      assert msg.getType() == ReefNetworkGroupCommProtos.GroupCommMessage.Type.UpdateTopology;
      assert effectiveTopology != null;
      LOG.entering("OperatorTopologyImpl.BaseTopologyUpdateHandler", "onNext", new Object[]{getQualifiedName(), msg});
      LOG.finest(getQualifiedName() + "Waiting to acquire topoLock");
      synchronized (topologyLock) {
        LOG.finest(getQualifiedName() + "Acquired topoLock");
        LOG.finest(getQualifiedName() + "Releasing topoLockAcquired CDL");
        topologyLockAcquired.countDown();
        try {
          updateBaseTopology();
          LOG.finest(getQualifiedName() + "Completed updating base & effective topologies");
        } catch (final ParentDeadException e) {
          throw new RuntimeException(getQualifiedName() + "BaseTopologyUpdateStage: Unexpected ParentDeadException", e);
        }
        updatingTopo.set(false);
        LOG.finest(getQualifiedName() + "Topology update complete. Notifying waiting threads");
        topologyLock.notifyAll();
        LOG.finest(getQualifiedName() + "Released topoLock");
      }
      LOG.exiting("OperatorTopologyImpl.BaseTopologyUpdateHandler", "onNext",
          Arrays.toString(new Object[]{getQualifiedName(), msg}));
    }
  }
}
