/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

/**
 *
 */
public class AllReduceOp<V> {

  private static final Logger LOG = Logger.getLogger(AllReduceOp.class.getName());

  private final Identifier self;
  private Identifier parent;
  private List<Identifier> children;
  private final Codec<V> codec;
  private final ReduceFunction<V> redFunc;
  private final AllReduceHandler handler;
  private final NetworkService<GroupCommMessage> netService;

  @Inject
  public AllReduceOp(NetworkService<GroupCommMessage> netService,
                     AllReduceHandler handler,
                     @Parameter(AllReduceConfig.DataCodec.class) Codec<V> codec,
                     @Parameter(AllReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
                     @Parameter(AllReduceConfig.SelfId.class) String selfId,
                     @Parameter(AllReduceConfig.ParentId.class) String parentId,
                     @Parameter(AllReduceConfig.ChildIds.class) Set<String> childIds,
                     @Parameter(AllReduceConfig.IdFactory.class) IdentifierFactory idFac) {
    this.netService = netService;
    this.handler = handler;
    this.codec = codec;
    this.redFunc = redFunc;
    this.parent = (parentId.equals(AllReduceConfig.defaultValue)) ? null : idFac.getNewInstance(parentId);
    this.self = (selfId.equals(AllReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));

    children = new ArrayList<>();
    LOG.log(Level.FINEST, "Received childIds:");
    for (String childId : childIds) {
      LOG.log(Level.FINEST, childId);
      if (childId.equals(AllReduceConfig.defaultValue)) {
        LOG.log(Level.FINEST, "Breaking");
        children = null;
        break;
      }
      children.add(idFac.getNewInstance(childId));
    }
  }

  /**
   * @param myData
   * @throws InterruptedException
   * @throws NetworkException
   */
  public V apply(V myData) throws NetworkFault, NetworkException, InterruptedException {
    LOG.log(Level.FINEST, "I am " + self.toString());

    V redVal = myData;
    if (children != null) {
      //I am an intermendiate node or root.
      //Wait for children to send
      List<V> vals = new ArrayList<>();
      vals.add(myData);
      List<Identifier> deadChildren = new ArrayList<>();
      for (Identifier child : children) {
        LOG.log(Level.FINEST, "Waiting for child: " + child);
        V cVal = handler.get(child, codec);
        LOG.log(Level.FINEST, "Received: " + cVal);
        if (cVal != null) {
          vals.add(cVal);
        } else {
          LOG.log(Level.FINEST, "Marking " + child + " as dead");
          deadChildren.add(child);
        }
      }
      children.removeAll(deadChildren);

      //Reduce the received values
      redVal = redFunc.apply(vals);
      LOG.log(Level.FINEST, "Local Reduced value: " + redVal);
      if (parent != null) {
        //I am no root.
        //send reduced value to parent
        //wait for the parent to aggregate
        //and send back
        LOG.log(Level.FINEST, "Sending " + redVal + " to parent: " + parent);
        send(redVal, parent);
        LOG.log(Level.FINEST, "Waiting for " + parent);
        V tVal = handler.get(parent, codec);
        LOG.log(Level.FINEST, "Received " + tVal + " from " + parent);
        if (tVal != null)
          redVal = tVal;
      }

      //Send the reduced value to children
      for (Identifier child : children) {
        LOG.log(Level.FINEST, "Sending " + redVal + " to child: " + child);
        send(redVal, child);
      }

    } else {
      //If I am root then we have
      //a one node tree. Nop
      if (parent != null) {
        //I am a leaf node.
        //Send and wait for
        //reduced val from parent
        Random toss = new Random();
        if (toss.nextFloat() < 0.7) {
          Thread.sleep(toss.nextInt(100) + 1);
          LOG.log(Level.FINEST, "I am marked to terminate and throw fault");
          throw new NetworkFault();
        }
        LOG.log(Level.FINEST, "I am leaf. Sending " + myData + " to my parent: " + parent);
        send(myData, parent);
        LOG.log(Level.FINEST, "Waiting for my parent: " + parent);
        V tVal = handler.get(parent, codec);
        LOG.log(Level.FINEST, "Received: " + tVal);
        if (tVal != null)
          redVal = tVal;
      }
    }
    LOG.log(Level.FINEST, "Returning:" + redVal);
    //Return reduced value
    return redVal;
  }

  public void send(V redVal, Identifier child) throws NetworkException {
    Connection<GroupCommMessage> link = netService.newConnection(child);
    link.open();
    link.write(Utils.bldGCM(Type.AllReduce, self, child, codec.encode(redVal)));
  }

}
