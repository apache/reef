/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.io.network.group.config;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.impl.GCMCodec;
import org.apache.reef.io.network.group.impl.operators.faulty.AllReduceConfig;
import org.apache.reef.io.network.group.impl.operators.faulty.AllReduceHandler;
import org.apache.reef.io.network.group.impl.operators.faulty.ExceptionHandler;
import org.apache.reef.io.network.group.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.impl.MessagingTransportFactory;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.impl.NetworkServiceParameters;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.network.util.Utils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.remote.Codec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final NetworkService<ReefNetworkGroupCommProtos.GroupCommMessage> ns;
  private final StringIdentifierFactory idFac = new StringIdentifierFactory();

  private final Map<ComparableIdentifier, Integer> taskIdMap = new HashMap<>();
  private final ComparableIdentifier[] tasks;
  private final int numTasks;
  private int runningTasks;

  public AllReduceManager(
      final Class<? extends Codec<T>> dataCodec, final Class<? extends ReduceFunction<T>> redFunc,
      final String nameServiceAddr, final int nameServicePort,
      final Map<ComparableIdentifier, Integer> id2port) {

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
        new MessagingTransportFactory(), new LoggingEventHandler<Message<ReefNetworkGroupCommProtos.GroupCommMessage>>(),
        new LoggingEventHandler<Exception>());
  }

  public synchronized double estimateVarInc(final ComparableIdentifier taskId) {
    final int childrenLost = getChildren(taskId) + 1;
    final double actVarDrop = 1.0 / this.numTasks;
    final double curVarDrop = 1.0 / (this.runningTasks - childrenLost);
    return (curVarDrop / actVarDrop) - 1;
  }

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

  public synchronized void remove(final ComparableIdentifier failedTaskId) {

    LOG.log(Level.FINEST, "All Reduce Manager removing " + failedTaskId);

    final ComparableIdentifier from = failedTaskId;
    final ComparableIdentifier to = this.tasks[parent(this.taskIdMap.get(failedTaskId))];

    final SingleThreadStage<ReefNetworkGroupCommProtos.GroupCommMessage> senderStage =
        new SingleThreadStage<>("SrcDeadMsgSender", new EventHandler<ReefNetworkGroupCommProtos.GroupCommMessage>() {
          @Override
          public void onNext(final ReefNetworkGroupCommProtos.GroupCommMessage srcDeadMsg) {
            final Connection<ReefNetworkGroupCommProtos.GroupCommMessage> link = ns.newConnection(to);
            try {
              link.open();
              LOG.log(Level.FINEST, "Sending source dead msg {0} to parent {1}", new Object[]{srcDeadMsg, to});
              link.write(srcDeadMsg);
            } catch (final NetworkException e) {
              LOG.log(Level.WARNING, "Unable to send failed task msg to parent: " + to, e);
              throw new RuntimeException("Unable to send failed task msg to parent: " + to, e);
            }
          }
        }, 5);

    final ReefNetworkGroupCommProtos.GroupCommMessage srcDeadMsg = Utils.bldGCM(ReefNetworkGroupCommProtos.GroupCommMessage.Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
    --this.runningTasks;
  }

  public List<ComparableIdentifier> getReceivers() {
    final int end = this.numTasks == 1 ? 1 : parent(this.numTasks);
    final List<ComparableIdentifier> retVal = new ArrayList<>(end);
    for (int i = 1; i <= end; i++) {
      retVal.add(this.tasks[i]);
    }
    return retVal;
  }

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

  public Configuration getConfig(final ComparableIdentifier taskId) {

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
   * Create {@link Configuration} for {@link org.apache.reef.io.network.group.impl.GroupCommNetworkHandler}
   * using base conf + list of identifiers
   */
  private Configuration createHandlerConf(final List<ComparableIdentifier> ids) {
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
   * @return per task {@link NetworkService} {@link Configuration} for the specified task
   */
  private Configuration createNetworkServiceConf(
      final String nameServiceAddr, final int nameServicePort, final Identifier self,
      final List<ComparableIdentifier> ids, final int nsPort) {
    final JavaConfigurationBuilder conf = tang.newConfigurationBuilder()
        .bindNamedParameter(TaskConfigurationOptions.Identifier.class, self.toString())
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, Integer.toString(nsPort));
    conf.addConfiguration(createHandlerConf(ids));
    return conf.build();
  }
}
