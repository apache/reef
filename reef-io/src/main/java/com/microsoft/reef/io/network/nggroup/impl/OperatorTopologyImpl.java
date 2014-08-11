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

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopology;
import com.microsoft.reef.io.network.nggroup.api.OperatorTopologyStruct;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.SingleThreadStage;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 */
public class OperatorTopologyImpl implements OperatorTopology {

  private static final Logger LOG = Logger.getLogger(OperatorTopologyImpl.class.getName());


  private static final byte[] emptyByte = new byte[0];


  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String selfId;
  private final String driverId;
  private final Sender sender;
  private final Object topologyLock = new Object();

  private final BlockingQueue<GroupCommMessage> deltas = new LinkedBlockingQueue<>();

  private OperatorTopologyStruct baseTopology;
  private OperatorTopologyStruct effectiveTopology;
  private CountDownLatch topologyLockAquired = new CountDownLatch(1);
  private final AtomicBoolean updatingTopo = new AtomicBoolean(false);

  private final EStage<GroupCommMessage> baseTopologyUpdateStage = new SingleThreadStage<>(new EventHandler<GroupCommMessage>() {

    @Override
    public void onNext(final GroupCommMessage msg) {
      assert (msg.getType() == Type.UpdateTopology);
      LOG.info(getQualifiedName() + "BaseTopologyUpdateStage received " + msg.getType() + " msg");
      synchronized (topologyLock) {
        LOG.info(getQualifiedName() + "Acquired topoLock");
        LOG.info(getQualifiedName() + "Releasing topoLoackAcquired CDL");
        topologyLockAquired.countDown();
        updateBaseTopology();
      }
    }
  }, 5);

  //The queue capacity might determine how many
  //tasks can be handled
  private final EStage<GroupCommMessage> dataHandlingStage = new SingleThreadStage<>("DataHandlingStage",
          new EventHandler<GroupCommMessage>() {

            @Override
            public void onNext(final GroupCommMessage dataMsg) {
              synchronized (topologyLock) {
                //Data msg
                while (updatingTopo.get()) {
                  try {
                    topologyLock.wait();
                  } catch (final InterruptedException e) {
                    throw new RuntimeException("InterruptedException while data handling" +
                    		"stage was waiting for updatingTopo to become false", e);
                  }
                }
                if (effectiveTopology != null) {
                  LOG.info(getQualifiedName()
                      + "Non-null effectiveTopo.addAsData(msg)");
                  effectiveTopology.addAsData(dataMsg);
                } else {
                  LOG.warning("Received a data message before effective topology was setup");
                }
              }
            }
          }, 10000);

  private final int version;

  @Inject
  public OperatorTopologyImpl(final Class<? extends Name<String>> groupName,
                              final Class<? extends Name<String>> operName, final String selfId, final String driverId,
                              final Sender sender, final int version) {
    super();
    this.groupName = groupName;
    this.operName = operName;
    this.selfId = selfId;
    this.driverId = driverId;
    this.sender = sender;
    this.version = version;
  }

  @Override
  public void handle(final GroupCommMessage msg) {
    final String srcId = msg.getSrcid();
    if (!msg.hasVersion()) {
      throw new RuntimeException(getQualifiedName() + "can only deal with versioned msgs");
    }
    final int msgVersion = msg.getVersion();
    LOG.info(getQualifiedName() + "Handling " + msg.getType() + " msg from ("
        + srcId + "," + msg.getSrcVersion() + ") for (" + msg.getDestid() + ","
        + msgVersion + ")");
    if (msgVersion < version) {
      LOG.warning(getQualifiedName() + "Received a ver-" + msgVersion
          + " msg while expecting ver-" + version + ". Discarding msg");
      return;
    }

    try {
      switch (msg.getType()) {
        case UpdateTopology:
          baseTopologyUpdateStage.onNext(msg);
          topologyLockAquired.await();
          updatingTopo.set(true);
          LOG.info(getQualifiedName() + "topoLockAquired CDL released. Resetting it to new CDL");
          //reset the Count Down Latch for the next update
          topologyLockAquired = new CountDownLatch(1);
          sendAckToDriver(msg);
          break;

        case TopologySetup:
          LOG.info(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);
          break;

        case ParentAdd:
        case ChildAdd:
          LOG.info(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);
          break;

        case ParentDead:
        case ChildDead:
          LOG.info(getQualifiedName() + "Adding to deltas queue");
          deltas.put(msg);
          //No synchronization is needed here
          //2 states: UpdatingTopo & NotUpdatingTopo
          //If UpdatingTopo, deltas.put still takes care of adding this msg to effTop
          //through baseTopo changes.
          //If not, we add to effTopo. So we are good.
          if (effectiveTopology != null) {
            LOG.info(getQualifiedName()
                + "Adding as data msg to non-null effective topology struct with msg");
            effectiveTopology.addAsData(msg);
            //          effectiveTopology.update(msg);
          } else {
            LOG.warning("Received a death message before effective topology was setup");
          }
          break;

        default:
          //This needs to be synchronized unlike Dead msgs
          //because data msgs are not routed through the base topo changes
          //So we need to make sure to wait for updateTopo to complete
          //and for the new effective topo to take effect. Hence updatinTopo
          //is set to false in refreshEffTopo.
          //Also, since this is called from a netty IO thread, we need to
          //create a stage to move the msgs from netty space to application space
          //and release the netty threads. Otherwise weird deadlocks can happen
          //Ex: Sent model to k nodes using broadcast. Send to K+1 th is waiting
          //for ACK. The K nodes already compute their states and reduce send
          //their results. If we haven't finished refreshEffTopo because of which
          //updatingTopo is true, we can't add the new msgs if the #netty threads is k
          //All k threads are waiting to add data. Single user thread that is waiting
          //for ACK does not come around to refreshEffTopo and we are deadlocked
          //because there aren't enough netty threads to dispatch
          //msgs to the application. Hence the stage
          dataHandlingStage.onNext(msg);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while trying to put ctrl msg into delta queue", e);
    }
  }

  @Override
  public void initialize() {
    refreshEffectiveTopology();
  }

  @Override
  public void sendToParent(final byte[] data, final Type msgType) {
    refreshEffectiveTopology();
    assert (effectiveTopology != null);
    effectiveTopology.sendToParent(data, msgType);
  }

  @Override
  public void sendToChildren(final byte[] data, final Type msgType) {
    refreshEffectiveTopology();
    assert (effectiveTopology != null);
    effectiveTopology.sendToChildren(data, msgType);
  }

  @Override
  public byte[] recvFromParent() {
    refreshEffectiveTopology();
    assert (effectiveTopology != null);
    return effectiveTopology.recvFromParent();
  }

  @Override
  public List<byte[]> recvFromChildren() {
    refreshEffectiveTopology();
    assert (effectiveTopology != null);
    return effectiveTopology.recvFromChildren();
  }

  @Override
  public <T> T recvFromChildren(final ReduceFunction<T> redFunc, final Codec<T> dataCodec) {
    refreshEffectiveTopology();
    assert (effectiveTopology != null);
    return effectiveTopology.recvFromChildren(redFunc, dataCodec);
  }

  private void refreshEffectiveTopology() {
    LOG.info(getQualifiedName() + "Refreshing effTopo");
    synchronized (topologyLock) {
      LOG.info(getQualifiedName() + "Acquired topoLock");
      if (baseTopology == null) {
        LOG.info(getQualifiedName() + "Base topo null. Creating");
        createBaseTopology();
      }
      assert (baseTopology != null);
      updateEffTopologyFromBaseTopology();
      assert (effectiveTopology != null);
      final Set<GroupCommMessage> deletionDeltas = new HashSet<>();
      copyDeletionDeltas(deletionDeltas);
      LOG.info(getQualifiedName() + "Updating effective topology struct with deletion msgs");
      effectiveTopology.update(deletionDeltas);
      updatingTopo.set(false);
      topologyLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Relinquished topoLock");
  }

  /**
   *
   */
  private void createBaseTopology() {
    baseTopology = new OperatorTopologyStructImpl(groupName, operName, selfId, driverId, sender, version);
    updateBaseTopology();
  }

  private void updateBaseTopology() {
    try {
      LOG.info(getQualifiedName() + "Updating base topology and setting dirty bit");
      baseTopology.setChanges(true);
      while (true) {
        LOG.info(getQualifiedName() + "Waiting for ctrl msgs");
        final GroupCommMessage msg = deltas.take();
        LOG.info(getQualifiedName() + "Got " + msg.getType() + " msg from " + msg.getSrcid());
        if (msg.getType() == Type.TopologySetup) {
          if (!deltas.isEmpty()) {
            LOG.warning("The delta msg queue is not empty when I got " + msg.getType() + ". Something is fishy!!!!");
          }
          break;
        }
        LOG.info(getQualifiedName() + "Updating basetopology struct");
        baseTopology.update(msg);
        sendAckToDriver(msg);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for delta msg from driver", e);
    }
  }

  /**
   * @param msg
   */
  private void sendAckToDriver(final GroupCommMessage msg) {
    try {
      LOG.info(getQualifiedName() + "Sending ACK to driver " + driverId);
      final String srcId = msg.getSrcid();
      if (!msg.hasVersion()) {
        throw new RuntimeException(getQualifiedName() + "Ack Sender can only deal with versioned msgs");
      }
      final int srcVersion = msg.getSrcVersion();
      switch (msg.getType()) {
        case UpdateTopology:
          LOG.info(getQualifiedName() + "Sending TopologySetup msg to driver");
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              Type.TopologySetup, selfId, this.version, driverId, srcVersion, emptyByte));
          break;
        case ParentAdd:
          LOG.info(getQualifiedName() + "Sending ParentAdded msg for " + srcId);
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              Type.ParentAdded, selfId, this.version, srcId, srcVersion, emptyByte), driverId);
          break;
        case ParentDead:
          LOG.info(getQualifiedName() + "Sending ParentRemoved msg for " + srcId);
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              Type.ParentRemoved, selfId, this.version, srcId, srcVersion, emptyByte), driverId);
          break;
        case ChildAdd:
          LOG.info(getQualifiedName() + "Sending ChildAdded msg for " + srcId);
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              Type.ChildAdded, selfId, this.version, srcId, srcVersion, emptyByte), driverId);
          break;
        case ChildDead:
          LOG.info(getQualifiedName() + "Sending ChildRemoved msg for " + srcId);
          sender.send(Utils.bldVersionedGCM(groupName, operName,
              Type.ChildRemoved, selfId, this.version, srcId, srcVersion, emptyByte), driverId);
          break;
        default:
          LOG.warning("Received a non control message for acknowledgement");
          throw new RuntimeException("Received a non control message for acknowledgement");
      }
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending ack to driver for delta msg " + msg.getType(), e);
    }
  }

  /**
   *
   */
  private void updateEffTopologyFromBaseTopology() {
    LOG.info(getQualifiedName() + "Updaing effective topology");
    if (baseTopology.hasChanges()) {
      LOG.info(getQualifiedName() + "Basetopo has changes");
      effectiveTopology = createFrom(baseTopology);
      LOG.info(getQualifiedName() + "Making basetopology tidy");
      baseTopology.setChanges(false);
    } else {
      LOG.info(getQualifiedName() + "Base topology has no changes");
    }
  }

  /**
   * @param topology
   * @return
   */
  private OperatorTopologyStruct createFrom(final OperatorTopologyStruct topology) {
    LOG.info(getQualifiedName() + "Creating effective topology from base");
    return new OperatorTopologyStructImpl(topology);
  }

  /**
   * @param deletionDeltas
   */
  private void copyDeletionDeltas(final Set<GroupCommMessage> deletionDeltas) {
    for (final GroupCommMessage msg : deltas) {
      final Type msgType = msg.getType();
      if (msgType == Type.ChildDead || msgType == Type.ParentDead) {
        LOG.info(getQualifiedName() + "Adding src dead msg from " + msg.getSrcid());
        deletionDeltas.add(msg);
      }
    }
  }

  @Override
  public String getSelfId() {
    return selfId;
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + ":" + Utils.simpleName(operName) + ":"
        + selfId + ":ver(" + version + ") - ";
  }

}
