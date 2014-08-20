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

import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.operators.faulty.AllReduceConfig;
import com.microsoft.reef.io.network.group.impl.operators.faulty.AllReduceHandler;
import com.microsoft.reef.io.network.group.impl.operators.faulty.ExceptionHandler;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.impl.MessagingTransportFactory;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.remote.Codec;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Deprecated
public class AllReduceManager<T> {

  private static final Logger LOG = Logger.getLogger(AllReduceManager.class.getName());

  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  private final Configuration allRedBaseConf;

  /**
   * Common configs
   */
  private final Class<? extends Codec<?>> dataCodecClass;
  private final Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * {@link NetworkService} related configs
   */
  private final String nameServiceAddr;
  private final int nameServicePort;
  private final Map<ComparableIdentifier, Integer> id2port;
  private final NetworkService<GroupCommMessage> ns;
  private final StringIdentifierFactory idFac = new StringIdentifierFactory();

  private final Map<ComparableIdentifier, Integer> taskIdMap = new HashMap<>();
  private final ComparableIdentifier[] tasks;
  private final int numTasks;
  private int runningTasks;

  /**
   * @param dataCodec
   * @param redFunc
   * @param nameServiceAddr
   * @param nameServicePort
   * @param id2port
   * @throws BindException
   */
  public AllReduceManager(
      final Class<? extends Codec<T>> dataCodec, final Class<? extends ReduceFunction<T>> redFunc,
      final String nameServiceAddr, final int nameServicePort,
      final Map<ComparableIdentifier, Integer> id2port) throws BindException {

    this.dataCodecClass = dataCodec;
    this.redFuncClass = redFunc;
    this.nameServiceAddr = nameServiceAddr;
    this.nameServicePort = nameServicePort;
    this.id2port = id2port;

    int i = 1;
    this.tasks = new ComparableIdentifier[id2port.size() + 1];
    for (final ComparableIdentifier id : id2port.keySet()) {
      this.tasks[i] = id;
      this.taskIdMap.put(id, i++);
    }

    this.numTasks = this.tasks.length - 1;
    this.runningTasks = this.numTasks;

    this.allRedBaseConf = tang.newConfigurationBuilder()
      .bindNamedParameter(AllReduceConfig.DataCodec.class, this.dataCodecClass)
      .bindNamedParameter(AllReduceConfig.ReduceFunction.class, this.redFuncClass)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class, GCMCodec.class)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class, AllReduceHandler.class)
      .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
      .bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServiceAddr)
      .bindNamedParameter(NameServerParameters.NameServerPort.class, Integer.toString(nameServicePort))
      .build();

    this.ns = new NetworkService<>(
        this.idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(), new LoggingEventHandler<Message<GroupCommMessage>>(),
        new LoggingEventHandler<Exception>());
  }

  /**
   * @param taskId
   * @return
   */
  public synchronized double estimateVarInc(final ComparableIdentifier taskId) {
    final int childrenLost = getChildren(taskId) + 1;
    final double actVarDrop = 1.0 / this.numTasks;
    final double curVarDrop = 1.0 / (this.runningTasks - childrenLost);
    return (curVarDrop / actVarDrop) - 1;
  }

  /**
   * @param taskId
   * @return
   */
  private synchronized int getChildren(final ComparableIdentifier taskId) {

    final int idx = this.taskIdMap.get(taskId);
    if (leftChild(idx) > this.numTasks) {
      return 0;
    }

    final int leftChildren = getChildren(this.tasks[leftChild(idx)]) + 1;
    if (rightChild(idx) > this.numTasks) {
      return leftChildren;
    }

    final int rightChildren = getChildren(this.tasks[rightChild(idx)]) + 1;
    return leftChildren + rightChildren;
  }

  /**
   * @param failedTaskId
   * @throws NetworkException
   */
  public synchronized void remove(final ComparableIdentifier failedTaskId) {

    LOG.log(Level.FINEST, "All Reduce Manager removing " + failedTaskId);

    final ComparableIdentifier from = failedTaskId;
    final ComparableIdentifier to = this.tasks[parent(this.taskIdMap.get(failedTaskId))];

    final SingleThreadStage<GroupCommMessage> senderStage =
          new SingleThreadStage<>("SrcDeadMsgSender", new EventHandler<GroupCommMessage>() {
      @Override
      public void onNext(final GroupCommMessage srcDeadMsg) {
        final Connection<GroupCommMessage> link = ns.newConnection(to);
        try {
          link.open();
          LOG.log(Level.FINEST, "Sending source dead msg {0} to parent {1}", new Object[] { srcDeadMsg, to });
          link.write(srcDeadMsg);
        } catch (final NetworkException e) {
          LOG.log(Level.WARNING, "Unable to send failed task msg to parent: " + to, e);
          throw new RuntimeException("Unable to send failed task msg to parent: " + to, e);
        }
      }
    }, 5);

    final GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
    --this.runningTasks;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getReceivers() {
    final int end = this.numTasks == 1 ? 1 : parent(this.numTasks);
    final List<ComparableIdentifier> retVal = new ArrayList<>(end);
    for (int i = 1; i <= end; i++) {
      retVal.add(this.tasks[i]);
    }
    return retVal;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getSenders() {
    final int start = this.numTasks == 1 ? 1 : parent(this.numTasks);
    final List<ComparableIdentifier> retVal = new ArrayList<>(this.numTasks - start);
    for (int i = start + 1; i <= this.numTasks; i++) {
      retVal.add(this.tasks[i]);
    }
    return retVal;
  }

  private int parent(final int i) {
    return i >> 1;
  }

  private int leftChild(final int i) {
    return i << 1;
  }

  private int rightChild(final int i) {
    return (i << 1) + 1;
  }

  /**
   * @param taskId
   * @return
   * @throws BindException
   */
  public Configuration getConfig(final ComparableIdentifier taskId) throws BindException {

    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(this.allRedBaseConf);
    jcb.bindNamedParameter(AllReduceConfig.SelfId.class, taskId.toString());

    final List<ComparableIdentifier> ids = new ArrayList<>();

    final int idx = this.taskIdMap.get(taskId);
    if (idx != 1) {
      final ComparableIdentifier par = this.tasks[parent(idx)];
      ids.add(par);
      jcb.bindNamedParameter(AllReduceConfig.ParentId.class, par.toString());
    }

    final int lcId = leftChild(idx);
    if (lcId <= this.numTasks) {
      final ComparableIdentifier lc = this.tasks[lcId];
      ids.add(lc);
      jcb.bindSetEntry(AllReduceConfig.ChildIds.class, lc.toString());
      final int rcId = rightChild(idx);
      if (rcId <= this.numTasks) {
        final ComparableIdentifier rc = this.tasks[rcId];
        ids.add(rc);
        jcb.bindSetEntry(AllReduceConfig.ChildIds.class, rc.toString());
      }
    }

    jcb.addConfiguration(createNetworkServiceConf(
        this.nameServiceAddr, this.nameServicePort, taskId, ids, this.id2port.get(taskId)));

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
  private Configuration createHandlerConf(final List<ComparableIdentifier> ids) throws BindException {
    final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    for (final ComparableIdentifier comparableIdentifier : ids) {
      jcb.bindSetEntry(AllReduceHandler.IDs.class, comparableIdentifier.toString());
    }
    return jcb.build();
  }

  /**
   * Create {@link NetworkService} {@link Configuration} for each task
   * using base conf + per task parameters
   *
   * @param nameServiceAddr
   * @param nameServicePort
   * @param self
   * @param ids
   * @param nsPort
   * @return per task {@link NetworkService} {@link Configuration} for the specified task
   * @throws BindException
   */
  private Configuration createNetworkServiceConf(
      final String nameServiceAddr, final int nameServicePort, final Identifier self,
      final List<ComparableIdentifier> ids, final int nsPort) throws BindException {
    final JavaConfigurationBuilder conf = tang.newConfigurationBuilder()
        .bindNamedParameter(TaskConfigurationOptions.Identifier.class, self.toString())
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, Integer.toString(nsPort));
    conf.addConfiguration(createHandlerConf(ids));
    return conf.build();
  }
}
