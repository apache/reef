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

import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.AnyOper;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 *
 */
public class MsgAggregator implements EventHandler<GroupCommMessage> {

  private static final Logger LOG = Logger.getLogger(MsgAggregator.class
      .getName());

  private int numTopologies;

  private final EStage<GroupCommMessage> senderStage;
  private final ConcurrentLinkedQueue<GroupCommMessage> addedMsgs = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<GroupCommMessage> topoUpdatedMsgs = new ConcurrentLinkedQueue<>();

  public MsgAggregator(final EStage<GroupCommMessage> senderStage) {
    this.senderStage = senderStage;
  }

  /**
   * @param numTopologies the numTopologies to set
   */
  public void setNumTopologies(final int numTopologies) {
    this.numTopologies = numTopologies;
  }

  @Override
  public void onNext(final GroupCommMessage msg) {
    // TODO Auto-generated method stub
    if (msg.getType().equals(Type.TopologyUpdated)) {
      topoUpdatedMsgs.add(msg);
      if (topoUpdatedMsgs.size() == numTopologies) {
        aggregateNSend(topoUpdatedMsgs);
      }
    } else {
      addedMsgs.add(msg);
    }
  }

  public void aggregateNSend() {
    aggregateNSend(addedMsgs);
  }

  /**
   *
   */
  private void aggregateNSend(final ConcurrentLinkedQueue<GroupCommMessage> msgQue) {
    if (msgQue.isEmpty()) {
      return;
    }
    final Map<String, Integer> dstVersions = new HashMap<>();
    final Map<String, List<GroupCommMessage>> perTaskMsgMap = new HashMap<>();

    for (final GroupCommMessage msg : msgQue) {
      final String dstId = msg.getDestid();
      final int dstVersion = msg.getVersion();
      if (!dstVersions.containsKey(dstId)) {
        dstVersions.put(dstId, dstVersion);
      } else {
        final int storedVersion = dstVersions.get(dstId);
        if (storedVersion != dstVersion) {
          LOG.warning("Found a dst with 2 versions: " + storedVersion + "," + dstVersion);
        }
        throw new RuntimeException("Version mismatch while aggregating msgs.");
      }
      final List<GroupCommMessage> msgs;
      if (!perTaskMsgMap.containsKey(dstId)) {
        msgs = new ArrayList<>();
        perTaskMsgMap.put(dstId, msgs);
      } else {
        msgs = perTaskMsgMap.get(dstId);
      }

      msgs.add(msg);
    }
    final GCMCodec codec = new GCMCodec();
    final GroupCommMessage gcm = msgQue.peek();
    final Type msgType = getMsgType(gcm);
    for (final Map.Entry<String, List<GroupCommMessage>> mapEntry : perTaskMsgMap.entrySet()) {
      final String dstId = mapEntry.getKey();
      final List<GroupCommMessage> msgsLst = mapEntry.getValue();
      final byte[][] encodedMsgs = new byte[msgsLst.size()][];
      int i = 0;
      for (final GroupCommMessage msg : msgsLst) {
        encodedMsgs[i++] = codec.encode(msg);
      }
      senderStage.onNext(Utils.bldVersionedGCM(
          Utils.getClass(gcm.getGroupname()), AnyOper.class, msgType,
          "No Source", 0, dstId, dstVersions.get(dstId), encodedMsgs));
    }
    msgQue.clear();
  }

  private Type getMsgType(final GroupCommMessage gcm) {
    Type msgType;
    switch (gcm.getType()) {
      case ParentAdd:
      case ChildAdd:
        msgType = Type.SourceAdd;
        break;
      case ParentDead:
      case ChildDead:
        msgType = Type.SourceDead;
        break;
      case TopologyChanges:
        msgType = Type.TopologyChanges;
        break;
      case TopologyUpdated:
        msgType = Type.TopologyUpdated;
        break;
      case TopologySetup:
        msgType = Type.TopologySetup;
        break;
      case UpdateTopology:
        msgType = Type.UpdateTopology;
        break;
      default:
        final String msg = "Unknown msg type in MsgAggregator";
        LOG.warning(msg);
        throw new RuntimeException(msg);
    }
    return msgType;
  }

}
