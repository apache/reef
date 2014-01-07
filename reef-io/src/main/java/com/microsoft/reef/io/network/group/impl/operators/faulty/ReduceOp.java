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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

/**
 * 
 */
public class ReduceOp {
  public static class Sender<V>{
    private final Identifier self;
    private Identifier parent;
    private Set<Identifier> children;
    private final Codec<V> codec;
    private final ReduceFunction<V> redFunc;
    private final ReduceHandler handler;
    private final NetworkService<GroupCommMessage> netService;
    private final ExceptionHandler excHandler;
    private final boolean approxGrad;
    private final Map<Identifier, V> prevGradients;
    
    @Inject
    public Sender(NetworkService<GroupCommMessage> netService,
        ReduceHandler handler,
        @Parameter(BroadReduceConfig.ReduceConfig.DataCodec.class) Codec<V> codec,
        @Parameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
        @Parameter(BroadReduceConfig.ReduceConfig.Sender.SelfId.class) String selfId,
        @Parameter(BroadReduceConfig.ReduceConfig.Sender.ParentId.class) String parentId,
        @Parameter(BroadReduceConfig.ReduceConfig.Sender.ChildIds.class) Set<String> childIds,
        @Parameter(BroadReduceConfig.ReduceConfig.Sender.ApproximateGradient.class) boolean approxGrad,
        @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac,
        @Parameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class) EventHandler<Exception> excHandler){
      this.netService = netService;
      this.handler = handler;
      this.codec = codec;
      this.redFunc = redFunc;
      this.parent = (parentId.equals(BroadReduceConfig.defaultValue)) ? null : idFac.getNewInstance(parentId);
      this.approxGrad = approxGrad;
      if(this.approxGrad)
        prevGradients = new HashMap<>();
      else
        prevGradients = null;
          
      this.self = (selfId.equals(BroadReduceConfig.defaultValue) ? null : idFac.getNewInstance(selfId));
      this.excHandler = (ExceptionHandler) excHandler;

      children = new HashSet<>();
      System.out.println("Approximate Gradient: " + approxGrad);
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
          System.out.println("RedSender: Removing " + id + " from children of " + self);
          children.remove(id);
        }
        else if(status>0){
          if(children==null)
            children = new HashSet<>();
            System.out.println("RedSender: Adding " + id + " to children of " + self);
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
      System.out.println("I am Reduce sender" + self.toString());

      V redVal = myData;
      if(children!=null){
        //I am an intermendiate node
        //Wait for children to send
        List<V> vals = new ArrayList<>();
        vals.add(myData);

        for(Identifier child : children){
          System.out.println("Waiting for child: " + child);
          V cVal = handler.get(child,codec);
          System.out.println("Received: " + cVal);
          if(cVal!=null){
            vals.add(cVal);
            if(approxGrad){
              prevGradients.put(child,cVal);
            }
          }
          else{
            if(approxGrad){
              V prevGrad = prevGradients.get(child);
              if(prevGrad!=null)
                vals.add(prevGrad);
            }
          }
        }
        
        //Reduce the received values
        redVal = redFunc.apply(vals);
        System.out.println("Local Reduced value: " + redVal);
        
        assert(parent!=null);
        
        System.out.println("Sending " + redVal + " to parent: " + parent);
        send(redVal,parent);
      }
      else{
        
        assert(parent!=null);
        //I am a leaf node.
        //Send and wait for
        //reduced val from parent
        System.out.println("I am leaf. Sending "+ myData +" to my parent: " + parent);
        send(myData, parent);
      }
    }
    
    public void send(V redVal, Identifier child) throws NetworkException {
      if(excHandler.hasExceptions())
        throw new NetworkException("Unable to send msgs");
      Connection<GroupCommMessage> link = netService.newConnection(child);
      link.open();
      link.write(Utils.bldGCM(Type.Reduce, self, child, codec.encode(redVal)));
    }
  }
  
  public static class Receiver<V>{
    private final Identifier self;
    private Set<Identifier> children;
    private final Codec<V> codec;
    private final ReduceFunction<V> redFunc;
    private final ReduceHandler handler;
    
    @Inject
    public Receiver(NetworkService<GroupCommMessage> netService,
        ReduceHandler handler,
        @Parameter(BroadReduceConfig.ReduceConfig.DataCodec.class) Codec<V> codec,
        @Parameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class) ReduceFunction<V> redFunc,
        @Parameter(BroadReduceConfig.ReduceConfig.Receiver.SelfId.class) String selfId,
        @Parameter(BroadReduceConfig.ReduceConfig.Receiver.ParentId.class) String parentId,
        @Parameter(BroadReduceConfig.ReduceConfig.Receiver.ChildIds.class) Set<String> childIds,
        @Parameter(BroadReduceConfig.IdFactory.class) IdentifierFactory idFac){
      this.handler = handler;
      this.codec = codec;
      this.redFunc = redFunc;
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
          System.out.println("RedReceiver: Removing " + id + " from children of " + self);
          children.remove(id);
        }
        else if(status>0){
          if(children==null)
            children = new HashSet<>();
            System.out.println("RedReceiver: Adding " + id + " to children of " + self);
          children.add(id);
        }
        else{
          //No change. Need not worry
        }
      }
      if(children!=null && children.isEmpty())
        children = null;
    }
    
    public V reduce() throws NetworkException, InterruptedException{
      //I am root.
      System.out.println("I am root " + self);
      V redVal = null;
      if(children!=null){
        //Wait for children to send
        List<V> vals = new ArrayList<>();

        for(Identifier child : children){
          System.out.println("Waiting for child: " + child);
          V cVal = handler.get(child,codec);
          System.out.println("Received: " + cVal);
          if(cVal!=null){
            vals.add(cVal);
          }
        }
        
        //Reduce the received values
        redVal = redFunc.apply(vals);
        System.out.println("Local Reduced value: " + redVal);
      }
      return redVal;
    }

  }
}
