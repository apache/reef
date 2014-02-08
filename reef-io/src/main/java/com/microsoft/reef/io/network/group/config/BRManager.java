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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.config.TaskTree.Status;
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

  private TaskTree tree = null;

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

    ns = new NetworkService<>(
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
                    Identifier srcId = idFac.getNewInstance("ComputeGradientTask" + src);
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
    ns.registerId(driverId);
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
          throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
        }
      }
    }, 5);
  }

  public void close() throws Exception {
    senderStage.close();
    ns.close();
  }

  public int childrenSupported(final ComparableIdentifier taskId) {
    return tree.childrenSupported(taskId);
  }

  /**
   * @param taskId
   */
  public synchronized void add(final ComparableIdentifier taskId) {
    if(tree==null){
      //Controller
      System.out.println("Adding controller");
      tree = new TaskTreeImpl();
      tree.add(taskId);
    }
    else{
      System.out.println("Adding Compute task. First updating tree");
      //Compute Task
      //Update tree
      tree.add(taskId);
      //Will Send Control msg to parent when scheduled
    }
  }

  public Configuration getControllerContextConf(final ComparableIdentifier id) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, tree.neighbors(id), 0));
    return jcb.build();
  }

  /**
   * @param taskId
   * @return
   * @throws BindException
   */
  public Configuration getControllerActConf(final ComparableIdentifier taskId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();//reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Receiver.SelfId.class, taskId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Sender.SelfId.class, taskId.toString());
    List<ComparableIdentifier> children = tree.scheduledChildren(taskId);
    for (ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Receiver.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Sender.ChildIds.class, child.toString());
    }
    //jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, taskId, tree.scheduledNeighbors(taskId), 0));
    return jcb.build();
  }

  public Configuration getComputeContextConf(final ComparableIdentifier taskId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, tree.neighbors(taskId), 0));
    return jcb.build();
  }

  public Configuration getComputeActConf(final ComparableIdentifier taskId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();//reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.SelfId.class, taskId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.SelfId.class, taskId.toString());
    ComparableIdentifier parent = tree.parent(taskId);
    if(parent!=null && Status.SCHEDULED==tree.getStatus(parent)){
      jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.ParentId.class, tree.parent(taskId).toString());
      jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.ParentId.class, tree.parent(taskId).toString());
    }
    List<ComparableIdentifier> children = tree.scheduledChildren(taskId);
    for (ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Sender.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Receiver.ChildIds.class, child.toString());
    }
//    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, taskId, tree.scheduledNeighbors(taskId), 0));
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
   * Create {@link NetworkService} {@link Configuration} for each task
   * using base conf + per task parameters
   *
   * @param nameServiceAddr
   * @param nameServicePort
   * @param ids
   * @param nsPort
   * @return per task {@link NetworkService} {@link Configuration} for the specified task
   * @throws BindException
   */
  private Configuration createNetworkServiceConf(
      String nameServiceAddr, int nameServicePort,
      List<ComparableIdentifier> ids, int nsPort) throws BindException {
    JavaConfigurationBuilder jcb = tang
        .newConfigurationBuilder();

    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServicePort.class,
        Integer.toString(nsPort));

    jcb.addConfiguration(createHandlerConf(ids));
    return jcb.build();
  }

  /**
   * @param failedTaskId
   */
  public void remove(final ComparableIdentifier failedTaskId) {
    //Remove the node from the tree
    tree.remove(failedTaskId);
    //Send src dead msg when unscheduled
  }

  /**
   * @param taskId
   */
  public synchronized void schedule(final ComparableIdentifier taskId, final boolean reschedule) {

    if(Status.SCHEDULED==tree.getStatus(taskId))
      return;
    tree.setStatus(taskId, Status.SCHEDULED);
    //This will not work when failure
    //is in an intermediate node
    List<ComparableIdentifier> schNeighs = tree.scheduledNeighbors(taskId);
    if(!schNeighs.isEmpty()){
      for (ComparableIdentifier neighbor : schNeighs) {
        System.out.println("Adding " + taskId + " as neighbor of " + neighbor);
        sendSrcAddMsg(taskId, neighbor, reschedule);
      }
    }
    else{
      //TODO: I seem some friction between elasticity and fault tolerance
      //here. Because of elasticity I have the if checks here and
      //the logic is restricted to just the parent instead of the
      //neighbor. With a generic topology scheduling the tasks
      //needs to co-ordinated with how faults are handled. We need
      //to generalize this carefully
      final ComparableIdentifier parent = tree.parent(taskId);
      if(tree.parent(taskId)!=null){
        //Only for compute tasks
        System.out.println("Parent " + parent + " was alive while submitting.");
        System.out.println("While scheduling found that parent is not scheduled.");
        System.out.println("Sending Src Dead msg");
        sendSrcDeadMsg(parent, taskId);
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
   * @param runTaskId
   * @return
   */
  public List<ComparableIdentifier> tasksToSchedule(final ComparableIdentifier runTaskId) {
    List<ComparableIdentifier> children = tree.children(runTaskId);
    //This is needed if we want to consider
    //removing an arbitrary node in the middle
    /*List<ComparableIdentifier> schedChildren = tree.scheduledChildren(runTaskId);
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
   * @param failedTaskId
   */
  public synchronized void unschedule(final ComparableIdentifier failedTaskId) {
    System.out.println("BRManager unscheduling " + failedTaskId);
    tree.setStatus(failedTaskId, Status.UNSCHEDULED);
    //Send a Source Dead message
    ComparableIdentifier from = failedTaskId;
    for(ComparableIdentifier to : tree.scheduledNeighbors(failedTaskId)){
      sendSrcDeadMsg(from, to);
    }
  }

  private void sendSrcDeadMsg(ComparableIdentifier from, ComparableIdentifier to) {
    GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
  }

  /**
   * @param failedTaskId
   * @return
   */
  public boolean canReschedule(final ComparableIdentifier failedTaskId) {
    final ComparableIdentifier parent = tree.parent(failedTaskId);
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
