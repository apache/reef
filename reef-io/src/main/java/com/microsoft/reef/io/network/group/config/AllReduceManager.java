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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class AllReduceManager<T> {

  private static final Logger LOG = Logger.getLogger(AllReduceManager.class.getName());

  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  private Configuration allRedBaseConf;


  /**
   * Common configs
   */
  private Class<? extends Codec<?>> dataCodecClass;
  private Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * {@link NetworkService} related configs
   */
  private final String nameServiceAddr;
  private final int nameServicePort;
  private Map<ComparableIdentifier, Integer> id2port;
  private final NetworkService<GroupCommMessage> ns;
  private final StringIdentifierFactory idFac = new StringIdentifierFactory();
  private final ComparableIdentifier driverId = (ComparableIdentifier) idFac.getNewInstance("driver");


  private Map<ComparableIdentifier, Integer> taskIdMap;
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
  public AllReduceManager(Class<? extends Codec<T>> dataCodec, Class<? extends ReduceFunction<T>> redFunc,
                          String nameServiceAddr, int nameServicePort,
                          Map<ComparableIdentifier, Integer> id2port) throws BindException {
    dataCodecClass = dataCodec;
    redFuncClass = redFunc;
    this.nameServiceAddr = nameServiceAddr;
    this.nameServicePort = nameServicePort;
    this.id2port = id2port;
    taskIdMap = new HashMap<ComparableIdentifier, Integer>();
    int i = 1;
    tasks = new ComparableIdentifier[id2port.size() + 1];
    for (ComparableIdentifier id : id2port.keySet()) {
      tasks[i] = id;
      taskIdMap.put(id, i++);
    }
    numTasks = tasks.length - 1;
    runningTasks = numTasks;
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    jcb.bindNamedParameter(AllReduceConfig.DataCodec.class, dataCodecClass);
    jcb.bindNamedParameter(AllReduceConfig.ReduceFunction.class, redFuncClass);
    jcb.bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class,
        GCMCodec.class);
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServiceHandler.class,
        AllReduceHandler.class);
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServiceExceptionHandler.class,
        ExceptionHandler.class);
    jcb.bindNamedParameter(NameServerParameters.NameServerAddr.class,
        nameServiceAddr);
    jcb.bindNamedParameter(NameServerParameters.NameServerPort.class,
        Integer.toString(nameServicePort));
    allRedBaseConf = jcb.build();

    ns = new NetworkService<>(
        idFac, 0, nameServiceAddr, nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(), new LoggingEventHandler<Message<GroupCommMessage>>(),
        new LoggingEventHandler<Exception>());
  }

  /**
   * @param taskId
   * @return
   */
  public synchronized double estimateVarInc(final ComparableIdentifier taskId) {
    double actVarDrop = 1.0 / numTasks;
    int childrenLost = getChildren(taskId) + 1;
    double curVarDrop = 1.0 / (runningTasks - childrenLost);
    return (curVarDrop / actVarDrop) - 1;
  }

  /**
   * @param taskId
   * @return
   */
  private synchronized int getChildren(final ComparableIdentifier taskId) {
    int idx = taskIdMap.get(taskId);
    int leftChildren, rightChildren;
    if (leftChild(idx) > numTasks) {
      return 0;
    } else {
      leftChildren = getChildren(tasks[leftChild(idx)]) + 1;
      if (rightChild(idx) > numTasks) {
        return leftChildren;
      } else {
        rightChildren = getChildren(tasks[rightChild(idx)]) + 1;
      }
    }
    return leftChildren + rightChildren;
  }


  /**
   * @param failedTaskId
   * @throws NetworkException
   */
  public synchronized void remove(final ComparableIdentifier failedTaskId) {

    LOG.log(Level.FINEST, "All Reduce Manager removing " + failedTaskId);
    final ComparableIdentifier from = failedTaskId;
    final ComparableIdentifier to = tasks[parent(taskIdMap.get(failedTaskId))];

    final SingleThreadStage<GroupCommMessage> senderStage = new SingleThreadStage<>("SrcDeadMsgSender", new EventHandler<GroupCommMessage>() {

      @Override
      public void onNext(GroupCommMessage srcDeadMsg) {
        Connection<GroupCommMessage> link = ns.newConnection(to);
        try {
          link.open();
          LOG.log(Level.FINEST, "Sending source dead msg " + srcDeadMsg + " to parent " + to);
          link.write(srcDeadMsg);
        } catch (NetworkException e) {
          e.printStackTrace();
          throw new RuntimeException("Unable to send failed task msg to parent of " + to, e);
        }
      }
    }, 5);

    final GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
    --runningTasks;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getReceivers() {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    int end = (numTasks == 1) ? 1 : parent(numTasks);
    for (int i = 1; i <= end; i++)
      retVal.add(tasks[i]);
    return retVal;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getSenders() {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    int start = (numTasks == 1) ? 1 : parent(numTasks);
    for (int i = start + 1; i <= numTasks; i++)
      retVal.add(tasks[i]);
    return retVal;
  }

  private int parent(int i) {
    return i >> 1;
  }

  private int leftChild(int i) {
    return i << 1;
  }

  private int rightChild(int i) {
    return (i << 1) + 1;
  }

  /**
   * @param taskId
   * @return
   * @throws BindException
   */
  public Configuration getConfig(final ComparableIdentifier taskId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(allRedBaseConf);
    jcb.bindNamedParameter(AllReduceConfig.SelfId.class, taskId.toString());
    List<ComparableIdentifier> ids = new ArrayList<>();
    int idx = taskIdMap.get(taskId);
    if (idx != 1) {
      ComparableIdentifier par = tasks[parent(idx)];
      ids.add(par);
      jcb.bindNamedParameter(AllReduceConfig.ParentId.class, par.toString());
    }

    int lcId = leftChild(idx);
    if (lcId <= numTasks) {
      ComparableIdentifier lc = tasks[lcId];
      ids.add(lc);
      jcb.bindSetEntry(AllReduceConfig.ChildIds.class, lc.toString());
      int rcId = rightChild(idx);
      if (rcId <= numTasks) {
        ComparableIdentifier rc = tasks[rcId];
        ids.add(rc);
        jcb.bindSetEntry(AllReduceConfig.ChildIds.class, rc.toString());
      }
    }

    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, taskId, ids, id2port.get(taskId)));
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
      String nameServiceAddr, int nameServicePort, Identifier self,
      List<ComparableIdentifier> ids, int nsPort) throws BindException {
    JavaConfigurationBuilder jcb = tang
        .newConfigurationBuilder();

    jcb.bindNamedParameter(TaskConfigurationOptions.Identifier.class, self.toString());
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServicePort.class,
        Integer.toString(nsPort));

    jcb.addConfiguration(createHandlerConf(ids));
    return jcb.build();
  }
}
