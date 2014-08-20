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
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AllReduceOp<V> {

  private static final Logger LOG = Logger.getLogger(AllReduceOp.class.getName());

  private final Random toss = new Random();

  private final Identifier self;
  private final Identifier parent;
  private final List<Identifier> children;
  private final Codec<V> codec;
  private final ReduceFunction<V> redFunc;
  private final AllReduceHandler handler;
  private final NetworkService<GroupCommMessage> netService;

  @Inject
  public AllReduceOp(
      final NetworkService<GroupCommMessage> netService,
      final AllReduceHandler handler,
      final @Parameter(AllReduceConfig.DataCodec.class) Codec<V> codec,
      final @Parameter(AllReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
      final @Parameter(AllReduceConfig.SelfId.class) String selfId,
      final @Parameter(AllReduceConfig.ParentId.class) String parentId,
      final @Parameter(AllReduceConfig.ChildIds.class) Set<String> childIds,
      final @Parameter(AllReduceConfig.IdFactory.class) IdentifierFactory idFac) {

    this.netService = netService;
    this.handler = handler;
    this.codec = codec;
    this.redFunc = redFunc;

    this.parent = parentId.equals(AllReduceConfig.defaultValue) ?
        null : idFac.getNewInstance(parentId);

    this.self = selfId.equals(AllReduceConfig.defaultValue) ?
        null : idFac.getNewInstance(selfId);

    final List<Identifier> newChildren = new ArrayList<>();
    for (final String childId : childIds) {
      LOG.log(Level.FINEST, "Add child ID: {0}", childId);
      if (childId.equals(AllReduceConfig.defaultValue)) {
        break;
      }
      newChildren.add(idFac.getNewInstance(childId));
    }

    this.children = newChildren.isEmpty() ? null : newChildren;
  }

  /**
   * @param myData
   * @throws InterruptedException
   * @throws NetworkException
   */
  public V apply(final V myData) throws NetworkFault, NetworkException, InterruptedException {
    LOG.log(Level.FINEST, "I am {0}", this.self);
    final V redVal = this.children == null ? applyLeaf(myData) : applyMidNode(myData);
    LOG.log(Level.FINEST, "{0} returns {1}", new Object[] { this.self, redVal });
    return redVal;
  }

  /**
   * I am an intermediate node or root. Wait for children to send.
   */
  private V applyMidNode(final V myData)
      throws NetworkFault, NetworkException, InterruptedException {

    final List<Identifier> deadChildren = new ArrayList<>();
    final List<V> vals = new ArrayList<>();
    vals.add(myData);

    for (final Identifier child : this.children) {
      LOG.log(Level.FINEST, "Waiting for child: {0}", child);
      final V cVal = this.handler.get(child, this.codec);
      LOG.log(Level.FINEST, "Received: {0}", cVal);
      if (cVal != null) {
        vals.add(cVal);
      } else {
        LOG.log(Level.FINEST, "Marking {0} as dead", child);
        deadChildren.add(child);
      }
    }

    this.children.removeAll(deadChildren);

    // Reduce the received values

    V redVal = this.redFunc.apply(vals);
    LOG.log(Level.FINEST, "Local Reduced value: {0}", redVal);

    if (this.parent != null) {

      // I am no root. Send reduced value to parent.
      // Wait for the parent to aggregate and send back.

      LOG.log(Level.FINEST, "Sending {0} to parent: {1}",
          new Object[] { redVal, this.parent });

      send(redVal, this.parent);

      LOG.log(Level.FINEST, "Waiting for {0}", this.parent);
      final V tVal = this.handler.get(this.parent, this.codec);
      LOG.log(Level.FINEST, "Received {0} from {1}", new Object[] { tVal, this.parent });
      if (tVal != null) {
        redVal = tVal;
      }
    }

    // Send the reduced value to children:
    for (final Identifier child : this.children) {
      LOG.log(Level.FINEST, "Sending {0} to child: {1}", new Object[] { redVal, child });
      send(redVal, child);
    }

    return redVal;
  }

  /**
   * I am a leaf node. Send and wait for reduced val from parent.
   */
  private V applyLeaf(final V myData)
      throws NetworkFault, NetworkException, InterruptedException {

    if (parent == null) {
      return myData;
    }

    if (this.toss.nextFloat() < 0.7) {
      Thread.sleep(this.toss.nextInt(100) + 1);
      LOG.log(Level.FINEST, "I am marked to terminate and throw fault");
      throw new NetworkFault();
    }

    LOG.log(Level.FINEST, "I am leaf. Sending {0} to my parent: {1}",
        new Object[] { myData, this.parent });

    send(myData, this.parent);

    LOG.log(Level.FINEST, "Waiting for my parent: {0}", this.parent);
    final V tVal = this.handler.get(this.parent, this.codec);

    LOG.log(Level.FINEST, "Received: {0}", tVal);
    return tVal == null ? myData : tVal;
  }

  public void send(final V redVal, final Identifier child) throws NetworkException {
    final Connection<GroupCommMessage> link = netService.newConnection(child);
    link.open();
    link.write(Utils.bldGCM(Type.AllReduce, this.self, child, this.codec.encode(redVal)));
  }
}
