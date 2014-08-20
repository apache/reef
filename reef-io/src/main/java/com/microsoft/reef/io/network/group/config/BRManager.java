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
package com.microsoft.reef.io.network.group.config;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BRManager {
  private static final Logger LOG = Logger.getLogger(BRManager.class.getName());
  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  private final Configuration reduceBaseConf;

  /**
   * Common configs
   */
  private final Class<? extends Codec<?>> brDataCodecClass;
  private final Class<? extends Codec<?>> redDataCodecClass;
  private final Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * {@link NetworkService} related configs
   */
  private final String nameServiceAddr;
  private final int nameServicePort;
  private final NetworkService<GroupCommMessage> ns;
  private final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private final ComparableIdentifier driverId = (ComparableIdentifier) idFac.getNewInstance("driver");
  private final ConcurrentHashMap<Identifier, BlockingQueue<GroupCommMessage>> srcAdds = new ConcurrentHashMap<>();

  private final ThreadPoolStage<GroupCommMessage> senderStage;

  private TaskTree tree = null;

  public BRManager(
      final Class<? extends Codec<?>> brDataCodec, final Class<? extends Codec<?>> redDataCodec,
      final Class<? extends ReduceFunction<?>> redFunc, final String nameServiceAddr,
      final int nameServicePort) throws BindException {

    this.brDataCodecClass = brDataCodec;
    this.redDataCodecClass = redDataCodec;
    this.redFuncClass = redFunc;
    this.nameServiceAddr = nameServiceAddr;
    this.nameServicePort = nameServicePort;

    this.reduceBaseConf = tang.newConfigurationBuilder()
        .bindNamedParameter(BroadReduceConfig.BroadcastConfig.DataCodec.class, this.brDataCodecClass)
        .bindNamedParameter(BroadReduceConfig.ReduceConfig.DataCodec.class, this.redDataCodecClass)
        .bindNamedParameter(BroadReduceConfig.ReduceConfig.ReduceFunction.class, this.redFuncClass)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class, GCMCodec.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class, BroadRedHandler.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
        .bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServiceAddr)
        .bindNamedParameter(NameServerParameters.NameServerPort.class, Integer.toString(nameServicePort))
        .build();

    this.ns = new NetworkService<>(this.idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(), new EventHandler<Message<GroupCommMessage>>() {
      @Override
      public void onNext(final Message<GroupCommMessage> srcAddsMsg) {
        final GroupCommMessage srcAdds = srcAddsMsg.getData().iterator().next();
        assert (srcAdds.getType() == Type.SourceAdd);
        final SingleThreadStage<GroupCommMessage> sendReqSrcAdd =
            new SingleThreadStage<>(new EventHandler<GroupCommMessage>() {
          @Override
          public void onNext(final GroupCommMessage srcAddsInner) {
            final SerializableCodec<HashSet<Integer>> sc = new SerializableCodec<>();
            for (final GroupMessageBody body : srcAddsInner.getMsgsList()) {
              final Set<Integer> srcs = sc.decode(body.getData().toByteArray());
              LOG.log(Level.FINEST, "Received req to send srcAdd for {0}", srcs);
              for (final int src : srcs) {
                final Identifier srcId = idFac.getNewInstance("ComputeGradientTask" + src);
                BRManager.this.srcAdds.putIfAbsent(srcId, new LinkedBlockingQueue<GroupCommMessage>(1));
                final BlockingQueue<GroupCommMessage> msgQue = BRManager.this.srcAdds.get(srcId);
                try {
                  LOG.log(Level.FINEST, "Waiting for srcAdd msg from: {0}", srcId);
                  final GroupCommMessage srcAddMsg = msgQue.take();
                  LOG.log(Level.FINEST, "Found a srcAdd msg from: {0}", srcId);
                  BRManager.this.senderStage.onNext(srcAddMsg);
                } catch (final InterruptedException e) {
                  LOG.log(Level.WARNING, "Interrupted wait for: " + srcId, e);
                  throw new RuntimeException(e);
                }
              }
            }
          }
        }, 5);
        sendReqSrcAdd.onNext(srcAdds);
      }
    }, new LoggingEventHandler<Exception>());

    this.ns.registerId(this.driverId);

    this.senderStage = new ThreadPoolStage<>(
        "SrcCtrlMsgSender", new EventHandler<GroupCommMessage>() {
      @Override
      public void onNext(final GroupCommMessage srcCtrlMsg) {

        final Identifier id = BRManager.this.idFac.getNewInstance(srcCtrlMsg.getDestid());

        if (BRManager.this.tree.getStatus((ComparableIdentifier) id) != Status.SCHEDULED) {
          return;
        }

        final Connection<GroupCommMessage> link = BRManager.this.ns.newConnection(id);
        try {
          link.open();
          LOG.log(Level.FINEST, "Sending source ctrl msg {0} for {1} to {2}",
              new Object[] { srcCtrlMsg.getType(), srcCtrlMsg.getSrcid(), id });
          link.write(srcCtrlMsg);
        } catch (final NetworkException e) {
          LOG.log(Level.WARNING, "Unable to send ctrl task msg to parent " + id, e);
          throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
        }
      }
    }, 5);
  }

  public void close() throws Exception {
    this.senderStage.close();
    this.ns.close();
  }

  public int childrenSupported(final ComparableIdentifier taskId) {
    return this.tree.childrenSupported(taskId);
  }

  /**
   * @param taskId
   */
  public synchronized void add(final ComparableIdentifier taskId) {
    if (this.tree == null) {
      //Controller
      LOG.log(Level.FINEST, "Adding controller");
      this.tree = new TaskTreeImpl();
    } else {
      LOG.log(Level.FINEST, "Adding Compute task. First updating tree");
      //Compute Task
      //Update tree
      //Will Send Control msg to parent when scheduled
    }
    this.tree.add(taskId);
  }

  public Configuration getControllerContextConf(final ComparableIdentifier id) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(this.reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(
        this.nameServiceAddr, this.nameServicePort, this.tree.neighbors(id), 0));
    return jcb.build();
  }

  /**
   * @param taskId
   * @return
   * @throws BindException
   */
  public Configuration getControllerActConf(final ComparableIdentifier taskId) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(); // reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Receiver.SelfId.class, taskId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Sender.SelfId.class, taskId.toString());
    final List<ComparableIdentifier> children = this.tree.scheduledChildren(taskId);
    for (final ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Receiver.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Sender.ChildIds.class, child.toString());
    }
    //jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, taskId, tree.scheduledNeighbors(taskId), 0));
    return jcb.build();
  }

  public Configuration getComputeContextConf(final ComparableIdentifier taskId) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(this.reduceBaseConf);
    jcb.addConfiguration(createNetworkServiceConf(
        this.nameServiceAddr, this.nameServicePort, this.tree.neighbors(taskId), 0));
    return jcb.build();
  }

  public Configuration getComputeActConf(final ComparableIdentifier taskId) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();//reduceBaseConf);
    jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.SelfId.class, taskId.toString());
    jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.SelfId.class, taskId.toString());
    final ComparableIdentifier parent = this.tree.parent(taskId);
    if (parent != null && Status.SCHEDULED == this.tree.getStatus(parent)) {
      jcb.bindNamedParameter(BroadReduceConfig.ReduceConfig.Sender.ParentId.class, tree.parent(taskId).toString());
      jcb.bindNamedParameter(BroadReduceConfig.BroadcastConfig.Receiver.ParentId.class, this.tree.parent(taskId).toString());
    }
    final List<ComparableIdentifier> children = this.tree.scheduledChildren(taskId);
    for (final ComparableIdentifier child : children) {
      jcb.bindSetEntry(BroadReduceConfig.ReduceConfig.Sender.ChildIds.class, child.toString());
      jcb.bindSetEntry(BroadReduceConfig.BroadcastConfig.Receiver.ChildIds.class, child.toString());
    }
    // jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, taskId, tree.scheduledNeighbors(taskId), 0));
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
      final List<ComparableIdentifier> ids) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    for (final ComparableIdentifier comparableIdentifier : ids) {
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
      final String nameServiceAddr, final int nameServicePort,
      final List<ComparableIdentifier> ids, final int nsPort) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder()
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, Integer.toString(nsPort));
    jcb.addConfiguration(createHandlerConf(ids));
    return jcb.build();
  }

  /**
   * @param failedTaskId
   */
  public void remove(final ComparableIdentifier failedTaskId) {
    //Remove the node from the tree
    this.tree.remove(failedTaskId);
    //Send src dead msg when unscheduled
  }

  /**
   * @param taskId
   */
  public synchronized void schedule(final ComparableIdentifier taskId, final boolean reschedule) {

    if (Status.SCHEDULED == this.tree.getStatus(taskId)) {
      return;
    }

    this.tree.setStatus(taskId, Status.SCHEDULED);
    //This will not work when failure is in an intermediate node
    final List<ComparableIdentifier> schNeighs = this.tree.scheduledNeighbors(taskId);
    if (!schNeighs.isEmpty()) {
      for (final ComparableIdentifier neighbor : schNeighs) {
        LOG.log(Level.FINEST, "Adding {0} as neighbor of {1}", new Object[] { taskId, neighbor });
        sendSrcAddMsg(taskId, neighbor, reschedule);
      }
    } else {
      //TODO: I seem some friction between elasticity and fault tolerance
      //here. Because of elasticity I have the if checks here and
      //the logic is restricted to just the parent instead of the
      //neighbor. With a generic topology scheduling the tasks
      //needs to co-ordinated with how faults are handled. We need
      //to generalize this carefully
      final ComparableIdentifier parent = tree.parent(taskId);
      if (this.tree.parent(taskId) != null) {
        //Only for compute tasks
        LOG.log(Level.FINEST,
            "Parent {0} was alive while submitting.\n" +
            "While scheduling found that parent is not scheduled.\n" +
            "Sending Source Dead message.", parent);
        sendSrcDeadMsg(parent, taskId);
      }
    }
  }

  private void sendSrcAddMsg(final ComparableIdentifier from,
                             final ComparableIdentifier to, final boolean reschedule) {
    final GroupCommMessage srcAddMsg = Utils.bldGCM(Type.SourceAdd, from, to, new byte[0]);
    if (reschedule) {
      LOG.log(Level.FINEST, "SrcAdd from: {0} queued up", from);
      this.srcAdds.putIfAbsent(from, new LinkedBlockingQueue<GroupCommMessage>(1));
      final BlockingQueue<GroupCommMessage> msgQue = this.srcAdds.get(from);
      msgQue.add(srcAddMsg);
    } else {
      this.senderStage.onNext(srcAddMsg);
    }
  }

  /**
   * @param runTaskId
   * @return
   */
  public List<ComparableIdentifier> tasksToSchedule(final ComparableIdentifier runTaskId) {
    final List<ComparableIdentifier> children = tree.children(runTaskId);
    //This is needed if we want to consider
    //removing an arbitrary node in the middle
    /*List<ComparableIdentifier> schedChildren = tree.scheduledChildren(runTaskId);
    for (ComparableIdentifier schedChild : schedChildren) {
      children.remove(schedChild);
    }*/
    final List<ComparableIdentifier> completedChildren = new ArrayList<>();
    for (final ComparableIdentifier child : children) {
      if (Status.COMPLETED == this.tree.getStatus(child)) {
        completedChildren.add(child);
      }
    }
    children.removeAll(completedChildren);
    return children;
  }


  /**
   * @param failedTaskId
   */
  public synchronized void unschedule(final ComparableIdentifier failedTaskId) {
    LOG.log(Level.FINEST, "BRManager unscheduling: {0}", failedTaskId);
    this.tree.setStatus(failedTaskId, Status.UNSCHEDULED);
    //Send a Source Dead message
    final ComparableIdentifier from = failedTaskId;
    for (final ComparableIdentifier to : this.tree.scheduledNeighbors(failedTaskId)) {
      sendSrcDeadMsg(from, to);
    }
  }

  private void sendSrcDeadMsg(final ComparableIdentifier from, final ComparableIdentifier to) {
    final GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    this.senderStage.onNext(srcDeadMsg);
  }

  /**
   * @param failedTaskId
   * @return
   */
  public boolean canReschedule(final ComparableIdentifier failedTaskId) {
    final ComparableIdentifier parent = tree.parent(failedTaskId);
    return parent != null && Status.SCHEDULED == this.tree.getStatus(parent);
  }

  /**
   * @param id
   */
  public void complete(final ComparableIdentifier id) {
    //Not unscheduling here since
    //unschedule needs to be specifically
    //called by the driver
    this.tree.setStatus(id, Status.COMPLETED);
  }
}
