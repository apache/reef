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

import com.microsoft.reef.driver.activity.ActivityConfigurationOptions;
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

/**
 *
 */
public class AllReduceManager<T> {

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


  private Map<ComparableIdentifier, Integer> actIdMap;
  private final ComparableIdentifier[] activities;
  private final int numActivities;
  private int runningActivities;

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
    actIdMap = new HashMap<ComparableIdentifier, Integer>();
    int i = 1;
    activities = new ComparableIdentifier[id2port.size() + 1];
    for (ComparableIdentifier id : id2port.keySet()) {
      activities[i] = id;
      actIdMap.put(id, i++);
    }
    numActivities = activities.length - 1;
    runningActivities = numActivities;
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
   * @param activity
   * @return
   */
  public synchronized double estimateVarInc(ComparableIdentifier activity) {
    double actVarDrop = 1.0 / numActivities;
    int childrenLost = getChildren(activity) + 1;
    double curVarDrop = 1.0 / (runningActivities - childrenLost);
    return (curVarDrop / actVarDrop) - 1;
  }

  /**
   * @param activity
   * @return
   */
  private synchronized int getChildren(ComparableIdentifier activity) {
    int idx = actIdMap.get(activity);
    int leftChildren, rightChildren;
    if (leftChild(idx) > numActivities) {
      return 0;
    } else {
      leftChildren = getChildren(activities[leftChild(idx)]) + 1;
      if (rightChild(idx) > numActivities) {
        return leftChildren;
      } else {
        rightChildren = getChildren(activities[rightChild(idx)]) + 1;
      }
    }
    return leftChildren + rightChildren;
  }


  /**
   * @param failedActId
   * @throws NetworkException
   */
  public synchronized void remove(ComparableIdentifier failedActId) {
    System.out.println("All Reduce Manager removing " + failedActId);
    ComparableIdentifier from = failedActId;
    final ComparableIdentifier to = activities[parent(actIdMap.get(failedActId))];
    SingleThreadStage<GroupCommMessage> senderStage = new SingleThreadStage<>("SrcDeadMsgSender", new EventHandler<GroupCommMessage>() {

      @Override
      public void onNext(GroupCommMessage srcDeadMsg) {
        Connection<GroupCommMessage> link = ns.newConnection(to);
        try {
          link.open();
          System.out.println("Sending source dead msg " + srcDeadMsg + " to parent " + to);
          link.write(srcDeadMsg);
        } catch (NetworkException e) {
          e.printStackTrace();
          throw new RuntimeException("Unable to send failed activity msg to parent of " + to, e);
        }
      }
    }, 5);
    GroupCommMessage srcDeadMsg = Utils.bldGCM(Type.SourceDead, from, to, new byte[0]);
    senderStage.onNext(srcDeadMsg);
    --runningActivities;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getReceivers() {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    int end = (numActivities == 1) ? 1 : parent(numActivities);
    for (int i = 1; i <= end; i++)
      retVal.add(activities[i]);
    return retVal;
  }

  /**
   * @return
   */
  public List<ComparableIdentifier> getSenders() {
    List<ComparableIdentifier> retVal = new ArrayList<>();
    int start = (numActivities == 1) ? 1 : parent(numActivities);
    for (int i = start + 1; i <= numActivities; i++)
      retVal.add(activities[i]);
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
   * @param actId
   * @return
   * @throws BindException
   */
  public Configuration getConfig(ComparableIdentifier actId) throws BindException {
    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(allRedBaseConf);
    jcb.bindNamedParameter(AllReduceConfig.SelfId.class, actId.toString());
    List<ComparableIdentifier> ids = new ArrayList<>();
    int idx = actIdMap.get(actId);
    if (idx != 1) {
      ComparableIdentifier par = activities[parent(idx)];
      ids.add(par);
      jcb.bindNamedParameter(AllReduceConfig.ParentId.class, par.toString());
    }

    int lcId = leftChild(idx);
    if (lcId <= numActivities) {
      ComparableIdentifier lc = activities[lcId];
      ids.add(lc);
      jcb.bindSetEntry(AllReduceConfig.ChildIds.class, lc.toString());
      int rcId = rightChild(idx);
      if (rcId <= numActivities) {
        ComparableIdentifier rc = activities[rcId];
        ids.add(rc);
        jcb.bindSetEntry(AllReduceConfig.ChildIds.class, rc.toString());
      }
    }

    jcb.addConfiguration(createNetworkServiceConf(nameServiceAddr, nameServicePort, actId, ids, id2port.get(actId)));
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

    jcb.bindNamedParameter(ActivityConfigurationOptions.Identifier.class, self.toString());
    jcb.bindNamedParameter(
        NetworkServiceParameters.NetworkServicePort.class,
        Integer.toString(nsPort));

    jcb.addConfiguration(createHandlerConf(ids));
    return jcb.build();
  }


}
