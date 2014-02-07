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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;

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

/**
 * 
 */
public class AllReduceOp<V> {
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
    System.out.println("Received childIds:");
    for(String childId : childIds){
      System.out.println(childId);
      if (childId.equals(AllReduceConfig.defaultValue)) {
        System.out.println("Breaking");
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
    System.out.println("I am " + self.toString());

    V redVal = myData;
    if(children!=null){
      //I am an intermendiate node or root.
      //Wait for children to send
      List<V> vals = new ArrayList<>();
      vals.add(myData);
      List<Identifier> deadChildren = new ArrayList<>();
      for(Identifier child : children){
        System.out.println("Waiting for child: " + child);
        V cVal = handler.get(child,codec);
        System.out.println("Received: " + cVal);
        if(cVal!=null){
          vals.add(cVal);
        }
        else{
          System.out.println("Marking " + child + " as dead");
          deadChildren.add(child);
        }
      }
      children.removeAll(deadChildren);
      
      //Reduce the received values
      redVal = redFunc.apply(vals);
      System.out.println("Local Reduced value: " + redVal);
      if(parent!=null){
        //I am no root.
        //send reduced value to parent
        //wait for the parent to aggregate
        //and send back
        System.out.println("Sending " + redVal + " to parent: " + parent);
        send(redVal,parent);
        System.out.println("Waiting for " + parent);
        V tVal = handler.get(parent,codec);
        System.out.println("Received " + tVal + " from " + parent);
        if(tVal!=null)
          redVal = tVal;
      }
      
      //Send the reduced value to children
      for(Identifier child : children){
        System.out.println("Sending " + redVal + " to child: " + child);
        send(redVal, child);
      }

    }
    else{
      //If I am root then we have
      //a one node tree. Nop
      if(parent!=null){
        //I am a leaf node.
        //Send and wait for
        //reduced val from parent
        Random toss = new Random();
        if(toss.nextFloat()<0.7){
          Thread.sleep(toss.nextInt(100)+1);
          System.out.println("I am marked to terminate and throw fault");
          throw new NetworkFault();
        }
        System.out.println("I am leaf. Sending "+ myData +" to my parent: " + parent);
        send(myData, parent);
        System.out.println("Waiting for my parent: " + parent);
        V tVal = handler.get(parent,codec);
        System.out.println("Received: " + tVal);
        if(tVal!=null)
          redVal = tVal;
      }
    }
    System.out.println("Returning:" + redVal);
    //Return reduced value
    return redVal;
  }
  
  public void send(V redVal, Identifier child) throws NetworkException {
    Connection<GroupCommMessage> link = netService.newConnection(child);
    link.open();
    link.write(Utils.bldGCM(Type.AllReduce, self, child, codec.encode(redVal)));
  }

}
