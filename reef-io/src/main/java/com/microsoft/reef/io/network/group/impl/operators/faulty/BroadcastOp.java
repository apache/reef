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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

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

/**
 * 
 */
public class BroadcastOp {
  public static class Sender<V>{
    private final Identifier self;
    private Set<Identifier> children;
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

      children = new HashSet<>();
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
    
    public void sync(){
      //TODO: Currently does not care about parents
      //Assumes that all ctr msgs are about children
      //There is currently only one case where ctrl
      //msgs are sent about parents thats when the
      //task is complete or control activity finishes
      //It works now because we do not depend on sync
      //functionality for that
      Map<Identifier, Integer> isIdAlive = new HashMap<>();
      handler.sync(isIdAlive);
      for(Identifier id : isIdAlive.keySet()){
        int status = isIdAlive.get(id);
        if(status<0){
          assert(children!=null);
          System.out.println("BroadSender: Removing " + id + " from children of " + self);
          children.remove(id);
        }
        else if(status>0){
          if(children==null)
            children = new HashSet<>();
            System.out.println("BroadSender: Adding " + id + " to children of " + self);
          children.add(id);
        }
        else{
          //No change. Need not worry
        }
      }
      if(children!=null && children.isEmpty())
        children = null;
    }
    
    /**
     * @param myData
     * @throws InterruptedException 
     * @throws NetworkException 
     */
    public void send(V myData) throws NetworkFault, NetworkException, InterruptedException {
      //I am root.
      System.out.println("I am Broadcast sender" + self.toString());

      if(children!=null){
        System.out.println("Sending "  + myData + " to " + children);
        for(Identifier child : children){
          System.out.println("Sending " + myData + " to child: " + child);
          send(myData,child);
        }
      }
    }
    
    public void send(V data, Identifier child) throws NetworkException {
      Connection<GroupCommMessage> link = netService.newConnection(child);
      link.open();
      link.write(Utils.bldGCM(Type.Broadcast, self, child, codec.encode(data)));
    }
  }
  
  public static class Receiver<V>{
    private final Identifier self;
    private Identifier parent;
    private Set<Identifier> children;
    private final Codec<V> codec;
    private final BroadcastHandler handler;
    private final NetworkService<GroupCommMessage> netService;
    
    @Inject
    public Receiver(NetworkService<GroupCommMessage> netService,
        BroadcastHandler handler,
        @Parameter(BroadReduceConfig.BroadcastConfig.DataCodec.class) Codec<V> codec,
        @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.SelfId.class) String selfId,
        @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.ParentId.class) String parentId,
        @Parameter(BroadReduceConfig.BroadcastConfig.Receiver.ChildIds.class) Set<String> childIds,
        @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac){
      this.netService = netService;
      this.handler = handler;
      this.codec = codec;
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));
      this.parent = (parentId.equals(BroadReduceConfig.defaultValue)) ? null : idFac.getNewInstance(parentId);;

      children = new HashSet<>();
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
    
    public void sync(){
      //TODO: Currently does not care about parents
      //Assumes that all ctr msgs are about children
      //There is currently only one case where ctrl
      //msgs are sent about parents thats when the
      //task is complete or control activity finishes
      //It works now because we do not depend on sync
      //functionality for that
      Map<Identifier, Integer> isIdAlive = new HashMap<>();
      handler.sync(isIdAlive);
      for(Identifier id : isIdAlive.keySet()){
        int status = isIdAlive.get(id);
        if(status<0){
          assert(children!=null);
          System.out.println("BroadReceiver: Removing " + id + " from children of " + self);
          children.remove(id);
        }
        else if(status>0){
          if(children==null)
            children = new HashSet<>();
            System.out.println("BroadReceiver: Adding " + id + " to children of " + self);
          children.add(id);
        }
        else{
          //No change. Need not worry
        }
      }
      if(children!=null && children.isEmpty())
        children = null;
    }
    
    public Set<Identifier> getChildren() {
      return children;
    }

    public V receive() throws NetworkException, InterruptedException{
      //I am an intermediate node or leaf.
      V retVal = null;
      if(parent!=null){
        //Wait for parent to send
        System.out.println("Waiting for parent: " + parent);
        retVal = handler.get(parent,codec);
        System.out.println("Received: " + retVal);
        
        if(children!=null){
          //I am an intermediate node
          //Send recvd value to all children
          System.out.println("Sending "  + retVal + " to " + children);
          for(Identifier child : children){
            System.out.println("Sending " + retVal + " to child: " + child);
            send(retVal,child);
          }
        }
      }
      return retVal;
    }
    
    public void send(V data, Identifier child) throws NetworkException {
      Connection<GroupCommMessage> link = netService.newConnection(child);
      link.open();
      link.write(Utils.bldGCM(Type.Broadcast, self, child, codec.encode(data)));
    }

  }
}
