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
package com.microsoft.reef.io.network.group.impl.operators.faulty;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BroadcastOp {
  private static final Logger LOG = Logger.getLogger(BroadcastOp.class.getName());

  public static class Sender<V> {
    private final Identifier self;
    private final Set<Identifier> children = new HashSet<>();
    private final Codec<V> codec;
    private final BroadcastHandler handler;
    private final NetworkService<GroupCommMessage> netService;

    @Inject
    public Sender(NetworkService<GroupCommMessage> netService,
                  BroadcastHandler handler,
                  @Parameter(BroadReduceConfig.BroadcastConfig.DataCodec.class) Codec<V> codec,
                  @Parameter(BroadReduceConfig.BroadcastConfig.Sender.SelfId.class) String selfId,
                  @Parameter(BroadReduceConfig.BroadcastConfig.Sender.ParentId.class) String parentId,
                  @Parameter(BroadReduceConfig.BroadcastConfig.Sender.ChildIds.class) Set<String> childIds,
                  @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac) {
      this.netService = netService;
      this.handler = handler;
      this.codec = codec;
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));

      LOG.log(Level.FINEST, "Received childIds:");
      for (final String childId : childIds) {
        LOG.log(Level.FINEST, childId);
        if (!childId.equals(AllReduceConfig.defaultValue)) {
          children.add(idFac.getNewInstance(childId));
        }
      }
    }

    public void sync() {
      final Map<Identifier, Integer> isIdAlive = new HashMap<>();
      this.handler.sync(isIdAlive);
      SyncHelper.update(this.children, isIdAlive, this.self);
    }

    /**
     * @param myData
     * @throws InterruptedException
     * @throws NetworkException
     */
    public void send(V myData) throws NetworkFault, NetworkException, InterruptedException {
      //I am root.
      LOG.log(Level.FINEST, "I am Broadcast sender" + self.toString());

      LOG.log(Level.FINEST, "Sending " + myData + " to " + children);
      for (final Identifier child : children) {
        LOG.log(Level.FINEST, "Sending " + myData + " to child: " + child);
        send(myData, child);
      }
    }

    private void send(final V data, final Identifier child) throws NetworkException {
      final Connection<GroupCommMessage> link = netService.newConnection(child);
      link.open();
      link.write(Utils.bldGCM(Type.Broadcast, self, child, codec.encode(data)));
    }
  }

  public static class Receiver<V> {
    private final Identifier self;
    private final Identifier parent;
    private final Set<Identifier> children = new HashSet<>();
    private final Codec<V> codec;
    private final BroadcastHandler handler;
    private final NetworkService<GroupCommMessage> netService;

    @Inject
    public Receiver(final NetworkService<GroupCommMessage> netService,
                    final BroadcastHandler handler,
                    final @Parameter(BroadReduceConfig.BroadcastConfig.DataCodec.class) Codec<V> codec,
                    final @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.SelfId.class) String selfId,
                    final @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.ParentId.class) String parentId,
                    final @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.ChildIds.class) Set<String> childIds,
                    final @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac) {
      this.netService = netService;
      this.handler = handler;
      this.codec = codec;
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));
      this.parent = (parentId.equals(BroadReduceConfig.defaultValue)) ? null : idFac.getNewInstance(parentId);

      LOG.log(Level.FINEST, "Received childIds:");
      for (final String childId : childIds) {
        LOG.log(Level.FINEST, childId);
        if (!childId.equals(AllReduceConfig.defaultValue)) {
          children.add(idFac.getNewInstance(childId));
        }

      }
    }

    public void sync() {
      final Map<Identifier, Integer> isIdAlive = new HashMap<>();
      this.handler.sync(isIdAlive);
      SyncHelper.update(this.children, isIdAlive, this.self);
    }

    public V receive() throws NetworkException, InterruptedException {
      //I am an intermediate node or leaf.
      final V retVal;
      if (this.parent != null) {
        //Wait for parent to send
        LOG.log(Level.FINEST, "Waiting for parent: " + parent);
        retVal = handler.get(parent, codec);
        LOG.log(Level.FINEST, "Received: " + retVal);

        LOG.log(Level.FINEST, "Sending " + retVal + " to " + children);
        for (final Identifier child : children) {
          LOG.log(Level.FINEST, "Sending " + retVal + " to child: " + child);
          this.send(retVal, child);
        }
      } else {
        retVal = null;
      }
      return retVal;
    }

    public void send(final V data, final Identifier child) throws NetworkException {
      final Connection<GroupCommMessage> link = netService.newConnection(child);
      link.open();
      link.write(Utils.bldGCM(Type.Broadcast, self, child, codec.encode(data)));
    }

  }
}
