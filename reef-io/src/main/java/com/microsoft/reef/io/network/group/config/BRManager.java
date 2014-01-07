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
package com.microsoft.reef.io.network.group.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.microsoft.reef.activity.Activity;
import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.config.ActivityTree.Status;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.operators.faulty.BroadRedHandler;
import com.microsoft.reef.io.network.group.impl.operators.faulty.BroadReduceConfig;
import com.microsoft.reef.io.network.group.impl.operators.faulty.ExceptionHandler;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.Codec;

/**
 * 
 */
public class BRManager {
  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();
  
  private Configuration reduceBaseConf; 

  /** Common configs */
  private Class<? extends Codec<?>> brDataCodecClass;
  private Class<? extends Codec<?>> redDataCodecClass;
  private Class<? extends ReduceFunction<?>> redFuncClass;
  
  /** {@link NetworkService} related configs */
  private final String nameServiceAddr;
  private final int nameServicePort;
  private final NetworkService<GroupCommMessage> ns;
  private final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private final ComparableIdentifier driverId = (ComparableIdentifier) idFac.getNewInstance("driver");
  private final ConcurrentHashMap<Identifier, BlockingQueue<GroupCommMessage>> srcAdds = new ConcurrentHashMap<>(); 
  
  private final ThreadPoolStage<GroupCommMessage> senderStage;
  
  private ActivityTree tree = null;
  
  public BRManager(Class<? extends Codec<?>> brDataCodec, Class<? extends Codec<?>> redDataCodec, Class<? extends ReduceFunction<?>> redFunc,
      String nameServiceAddr, int nameServicePort) throws BindException{
    brDataCodecClass = brDataCodec;
    redDataCodecClass = redDataCodec;
    redFuncClass = redFunc;
    this.nameServiceAddr = nameServiceAddr;
    this.nameServicePort = nameServicePort;
    
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.DataCodec.class, brDataCodecClass);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.DataCodec.class, redDataCodecClass);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class, redFuncClass);
    jcb.bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class,
        GCMCodec.class);
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServiceHandler.class,
        BroadRedHandler.class);
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServiceExceptionHandler.class,
        ExceptionHandler.class);
    jcb.bindNamedParameter(NameServerParameters.NameServerAddr.class,
        nameServiceAddr);
    jcb.bindNamedParameter(NameServerParameters.NameServerPort.class,
        Integer.toString(nameServicePort));
    reduceBaseConf = jcb.build();
    
    ns = new NetworkService<>(driverId.toString(),
        idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(), new EventHandler<Message<GroupCommMessage>>() {

          @Override
          public void onNext(Message<GroupCommMessage> srcAddsMsg) {
            GroupCommMessage srcAdds = srcAddsMsg.getData().iterator().next();
            assert(srcAdds.getType()==Type.SourceAdd);
            final SingleThreadStage<GroupCommMessage> sendReqSrcAdd = new SingleThreadStage<>(new EventHandler<GroupCommMessage>() {

              @Override
              public void onNext(GroupCommMessage srcAddsInner) {
                SerializableCodec<HashSet<Integer>> sc = new SerializableCodec<>();
                for(GroupMessageBody body : srcAddsInner.getMsgsList()){
                  Set<Integer> srcs = sc.decode(body.getData().toByteArray());
                  System.out.println("Received req to send srcAdd for " + srcs);
                  for (Integer src : srcs) {
                    Identifier srcId = idFac.getNewInstance("ComputeGradientActivity" + src);
                    BRManager.this.srcAdds.putIfAbsent(srcId, new LinkedBlockingQueue<GroupCommMessage>(1));
                    BlockingQueue<GroupCommMessage> msgQue = BRManager.this.srcAdds.get(srcId);
                    try {
                      System.out.println("Waiting for srcAdd msg from: " + srcId);
                      GroupCommMessage srcAddMsg = msgQue.take();
                      System.out.println("Found a srcAdd msg from: " + srcId);
                      senderStage.onNext(srcAddMsg);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                }
              }
            }, 5);
            sendReqSrcAdd.onNext(srcAdds);
          }
        },
        new LoggingEventHandler<Exception>());
    
    senderStage = new ThreadPoolStage<>("SrcCtrlMsgSender", new EventHandler<GroupCommMessage>() {

      @Override
      public void onNext(GroupCommMessage srcCtrlMsg) {
        Identifier id = idFac.getNewInstance(srcCtrlMsg.getDestid());
        
        if(tree.getStatus((ComparableIdentifier) id)!=Status.SCHEDULED)
          return;
        
        Connection<GroupCommMessage> link = ns.newConnection(id);
        try {
          link.open();
          System.out.println("Sending source ctrl msg " + srcCtrlMsg.getType() + " for "  + srcCtrlMsg.getSrcid() + " to " + id);
          link.write(srcCtrlMsg);
        } catch (NetworkException e) {
          e.printStackTrace();
          throw new RuntimeException("Unable to send ctrl activity msg to parent " + id, e);
        }
      }
    }, 5);
  }
  
  public void close() throws Exception{
    senderStage.close();
    ns.close();
  }
  
  public int childrenSupported(ComparableIdentifier actId){
    return tree.childrenSupported(actId);
  }

  /**
   * @param actId
   */
  public synchronized void add(ComparableIdentifier actId) {
    if(tree==null){
      //Controller
      System.out.println("Adding controller");
      tree = new ActivityTreeImpl();
      tree.add(actId);
    }
    else{
      System.out.println("Adding Compute activity. First updating tree");
      //Compute Activity
      //Update tree
      tree.add(actId);
      //Will Send Control msg to parent when scheduled
    }
  }
  
  public Configuration getControllerContextConf(ComparableIdentifier id) throws BindException{
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, id, tree.neighbors(id), 0));
    return jcb.build();
  }
  
  /**
   * @param controllerId
   * @return
   * @throws BindException 
   */
  public Configuration getControllerActConf(ComparableIdentifier actId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();//reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Receiver.SelfId.class, actId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Sender.SelfId.class, actId.toString());
    List<ComparableIdentifier> children = tree.scheduledChildren(actId);
    for (ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Receiver.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Sender.ChildIds.class, child.toString());
    }
    //jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, actId, tree.scheduledNeighbors(actId), 0));
    return jcb.build();
  }
  
  public Configuration getComputeContextConf(ComparableIdentifier actId) throws BindException{
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, actId, tree.neighbors(actId), 0));
    return jcb.build();
  }
  
  public Configuration getComputeActConf(ComparableIdentifier actId) throws BindException{
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();//reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.SelfId.class, actId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.SelfId.class, actId.toString());
    ComparableIdentifier parent = tree.parent(actId);
    if(parent!=null && Status.SCHEDULED==tree.getStatus(parent)){
      jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.ParentId.class, tree.parent(actId).toString());
      jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.ParentId.class, tree.parent(actId).toString());
    }
    List<ComparableIdentifier> children = tree.scheduledChildren(actId);
    for (ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Sender.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Receiver.ChildIds.class, child.toString());
    }
//    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, actId, tree.scheduledNeighbors(actId), 0));
    return jcb.build();
  }
  
  /**
   * Create {@link Configuration} for {@link GroupCommNetworkHandler}
   * using base conf + list of identifiers
   *
   * @param ids
   * @return
   * @throws BindException
   */
  private Configuration createHandlerConf(
      List<ComparableIdentifier> ids) throws BindException {
    JavaConfigurationBuilder jcb = tang
        .newConfigurationBuilder();
    for (ComparableIdentifier comparableIdentifier : ids) {
      jcb.bindSetEntry(BroadRedHandler.IDs.class, comparableIdentifier.toString());
    }
    return jcb.build();
  }

  /**
   * Create {@link NetworkService} {@link Configuration} for each activity
   * using base conf + per activity parameters
   *
   * @param nameServiceAddr
   * @param nameServicePort
   * @param self
   * @param ids
   * @param nsPort
   * @return per activity {@link NetworkService} {@link Configuration} for the specified activity
   * @throws BindException
   */
  private Configuration createNetworkServiceConf(
      String nameServiceAddr, int nameServicePort, Identifier self,
      List<ComparableIdentifier> ids, int nsPort) throws BindException {
    JavaConfigurationBuilder jcb = tang
        .newConfigurationBuilder();

//    jcb.bindNamedParameter(ActivityConfiguration.Identifier.class, self.toString());
    jcb.bindNamedParameter(NetworkServiceParameters.ActivityId.class, self.toString());
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServicePort.class,
        Integer.toString(nsPort));

    jcb.addConfiguration(createHandlerConf(ids));
    return jcb.build();
  }

  /**
   * @param actId
   */
  public void remove(ComparableIdentifier failedActId) {
    //Remove the node from the tree
    tree.remove(failedActId);
    //Send src dead msg when unscheduled
  }

  /**
   * @param actId
   */
  public synchronized void schedule(ComparableIdentifier actId, boolean reschedule) {

    if(Status.SCHEDULED==tree.getStatus(actId))
      return;
    tree.setStatus(actId, Status.SCHEDULED);
    //This will not work when failure
    //is in an intermediate node
    List<ComparableIdentifier> schNeighs = tree.scheduledNeighbors(actId);
    if(!schNeighs.isEmpty()){
      for (ComparableIdentifier neighbor : schNeighs) {
        System.out.println("Adding " + actId + " as neighbor of " + neighbor);
        sendSrcAddMsg(actId, neighbor, reschedule);
      }
    }
    else{
      //TODO: I seem some friction between elasticity and fault tolerance
      //here. Because of elasticity I have the if checks here and
      //the logic is restricted to just the parent instead of the
      //neighbor. With a generic topology scheduling the activities
      //needs to co-ordinated with how faults are handled. We need
      //to generalize this carefully
      final ComparableIdentifier parent = tree.parent(actId);
      if(tree.parent(actId)!=null){
        //Only for compute activities
        System.out.println("Parent " + parent + " was alive while submitting.");
        System.out.println("While scheduling found that parent is not scheduled.");
        System.out.println("Sending Src Dead msg");
        sendSrcDeadMsg(parent, actId);
      }
    }
  }

  private void sendSrcAddMsg(ComparableIdentifier from,
      final ComparableIdentifier to, boolean reschedule) {
    GroupCommMessage srcAddMsg = Utils.bldGCM(Type.SourceAdd, from, to, new byte[0]);
    if(!reschedule)
      senderStage.onNext(srcAddMsg);
    else{
      System.out.println("SrcAdd from: " + from + " queued up");
      srcAdds.putIfAbsent(from, new LinkedBlockingQueue<GroupCommMessage>(1));
      BlockingQueue<GroupCommMessage> msgQue = srcAdds.get(from);
      msgQue.add(srcAddMsg);
    }
  }

  /**
   * @param runAct
   * @return
   */
  public List<ComparableIdentifier> activitesToSchedule(
      ComparableIdentifier runAct) {
    List<ComparableIdentifier> children = tree.children(runAct);
    //This is needed if we want to consider
    //removing an arbitrary node in the middle
    /*List<ComparableIdentifier> schedChildren = tree.scheduledChildren(runAct);
    for (ComparableIdentifier schedChild : schedChildren) {
      children.remove(schedChild);
    }*/
    List<ComparableIdentifier> completedChildren = new ArrayList<>();
    for (ComparableIdentifier child : children) {
      if(Status.COMPLETED==tree.getStatus(child))
        completedChildren.add(child);
    }
    children.removeAll(completedChildren);
    return children;
  }

  
  /**
   * @param actId
   */
  public synchronized void unschedule(ComparableIdentifier failedActId) {
    System.out.println("BRManager unscheduling " + failedActId);
    tree.setStatus(failedActId, Status.UNSCHEDULED);
    //Send a Source Dead message
    ComparableIdentifier from = failedActId;
    for(ComparableIdentifier to : tree.scheduledNeighbors(failedActId)){
      sendSrcDeadMsg(from, to);
    }
  }

  private void sendSrcDeadMsg(ComparableIdentifier from, ComparableIdentifier to) {
    GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
  }

  /**
   * @param actId
   * @return
   */
  public boolean canReschedule(ComparableIdentifier failedActId) {
    final ComparableIdentifier parent = tree.parent(failedActId);
    if(parent!=null && Status.SCHEDULED==tree.getStatus(parent))
      return true;
    return false;
  }

  /**
   * @param id
   */
  public void complete(ComparableIdentifier id) {
    //Not unscheduling here since
    //unschedule needs to be specifically
    //called by the driver
    tree.setStatus(id, Status.COMPLETED);
  }
}
