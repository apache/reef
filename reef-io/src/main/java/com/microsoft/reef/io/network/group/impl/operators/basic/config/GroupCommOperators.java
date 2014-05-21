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
package com.microsoft.reef.io.network.group.impl.operators.basic.config;

import com.microsoft.reef.io.network.group.impl.ExceptionHandler;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.config.*;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.impl.NetworkServiceParameters;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.ComparableIdentifier;

import java.util.*;

/**
 * Helper class to create configurations of operators from their descriptions
 * <p/>
 * TODO: This should be turned into pure configuration module logic
 */
public class GroupCommOperators {
  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  public static final class NetworkServiceConfig extends ConfigurationModuleBuilder {
    public static final RequiredParameter<String> NAME_SERVICE_ADDRESS = new RequiredParameter<>();
    public static final RequiredParameter<Integer> NAME_SERVICE_PORT = new RequiredParameter<>();
    public static final RequiredParameter<String> ID_LIST_STRING = new RequiredParameter<>();
    public static final RequiredParameter<Integer> NETWORK_SERVICE_PORT = new RequiredParameter<>();
    public static final ConfigurationModule CONF = new NetworkServiceConfig()
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class, GCMCodec.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class, GroupCommNetworkHandler.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
        .bindImplementation(NetworkService.class, NetworkService.class)
        .bindImplementation(GroupCommNetworkHandler.class, GroupCommNetworkHandler.class)
        .bindNamedParameter(NameServerParameters.NameServerAddr.class, NAME_SERVICE_ADDRESS)
        .bindNamedParameter(NameServerParameters.NameServerPort.class, NAME_SERVICE_PORT)
        .bindNamedParameter(GroupCommNetworkHandler.IDs.class, ID_LIST_STRING)
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, NETWORK_SERVICE_PORT)
        .build();
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
  private static Configuration createNetworkServiceConf(
      String nameServiceAddr, int nameServicePort,
      List<ComparableIdentifier> ids, int nsPort) throws BindException {
    return
        NetworkServiceConfig.CONF
            .set(NetworkServiceConfig.NAME_SERVICE_ADDRESS, nameServiceAddr)
            .set(NetworkServiceConfig.NAME_SERVICE_PORT, nameServicePort)
            .set(NetworkServiceConfig.ID_LIST_STRING, Utils.listToString(ids))
            .set(NetworkServiceConfig.NETWORK_SERVICE_PORT, nsPort)
            .build();
  }

  /**
   * Return per task Configurations as a map given the operator descriptions
   *
   * @param opDesc
   * @param nameServiceAddr
   * @param nameServicePort
   * @param networkServicePorts
   * @return per task Configurations as a map given the operator descriptions
   * @throws BindException
   */
  public static Map<ComparableIdentifier, Configuration> getConfigurations(
      List<GroupOperatorDescription> opDesc, String nameServiceAddr,
      int nameServicePort,
      Map<ComparableIdentifier, Integer> networkServicePorts)
      throws BindException {
    Map<ComparableIdentifier, List<ComparableIdentifier>> sources = new HashMap<>();
    Set<ComparableIdentifier> tasks = new HashSet<>();

    OperatorConfigs opConfigs = new OperatorConfigs();
    for (GroupOperatorDescription groupOperatorDescription : opDesc) {
      switch (groupOperatorDescription.operatorType) {
        case SCATTER:
        case BROADCAST:
          handleRootSenderOp(tasks, opConfigs, groupOperatorDescription);
          break;


        case GATHER:
        case REDUCE:
          handleRootReceiverOp(tasks, opConfigs, groupOperatorDescription);
          break;

        case ALL_GATHER:
        case ALL_REDUCE:
        case REDUCE_SCATTER:
          handleSymmetricOp(tasks, opConfigs, groupOperatorDescription);
          break;

        default:
          break;
      }
    }

    // For each task store all other participating task ids
    for (final ComparableIdentifier taskId : tasks) {
      final List<ComparableIdentifier> srcsPerTask = new ArrayList<>(tasks);
      srcsPerTask.remove(taskId);
      sources.put(taskId, srcsPerTask);
    }

    // For each task merge the individual group operator config
    // with the network service config to create the full config
    final Map<ComparableIdentifier, Configuration> result = new HashMap<>();
    for (final Map.Entry<ComparableIdentifier, Integer> entry : networkServicePorts.entrySet()) {
      final ComparableIdentifier taskId = entry.getKey();
      final int nsPort = entry.getValue();
      final Configuration nsConf = createNetworkServiceConf(
          nameServiceAddr, nameServicePort, sources.get(taskId), nsPort);
      JavaConfigurationBuilder jcb = tang.newConfigurationBuilder(nsConf);
      opConfigs.addConfigurations(taskId, jcb);
      result.put(taskId, jcb.build());

    }
    return result;
  }

  /**
   * Handle creating configuration for a symmetric operation
   * Adds the operator configs for each task into {@link OperatorConfigs}
   * <p/>
   * For current implementation the underlying semantics are still asymmetric. So pick
   * a random task as root and others as leaves
   *
   * @param tasks
   * @param opConfigs
   * @param opDesc
   * @throws BindException
   */
  private static void handleSymmetricOp(
      Set<ComparableIdentifier> tasks,
      OperatorConfigs opConfigs,
      GroupOperatorDescription opDesc)
      throws BindException {
    SymmetricOpDescription symOp = (SymmetricOpDescription) opDesc;
    ComparableIdentifier dummyRoot = symOp.taskIds.get(0);
    tasks.add(dummyRoot);
    List<ComparableIdentifier> others = symOp.taskIds.subList(1, symOp.taskIds.size());
    tasks.addAll(others);

    Configuration recvConf = getRootConf(opDesc, dummyRoot, others);
    opConfigs.put(dummyRoot, recvConf);

    for (ComparableIdentifier sender : others) {
      Configuration senderConf = getLeafConf(opDesc, dummyRoot, sender);
      opConfigs.put(sender, senderConf);
    }
  }

  /**
   * Handle creating configuration for an operation whose root is a receiver
   * Adds the operator configs for each task into {@link OperatorConfigs}
   *
   * @param tasks
   * @param opConfigs
   * @param opDesc
   * @throws BindException
   */
  private static void handleRootReceiverOp(
      Set<ComparableIdentifier> tasks,
      OperatorConfigs opConfigs,
      GroupOperatorDescription opDesc)
      throws BindException {
    RootReceiverOp rootReceiverOp = (RootReceiverOp) opDesc;
    ComparableIdentifier receiverID = rootReceiverOp.receiver;
    tasks.add(receiverID);
    tasks.addAll(rootReceiverOp.senders);

    Configuration recvConf = getRootConf(opDesc, receiverID, rootReceiverOp.senders);
    opConfigs.put(receiverID, recvConf);

    for (ComparableIdentifier sender : rootReceiverOp.senders) {
      Configuration senderConf = getLeafConf(opDesc, receiverID, sender);
      opConfigs.put(sender, senderConf);
    }
  }

  /**
   * Handle creating configuration for an operation whose root is a sender
   * Adds the operator configs for each task into {@link OperatorConfigs}
   *
   * @param tasks
   * @param opConfigs
   * @param opDesc
   * @throws BindException
   */
  private static void handleRootSenderOp(
      Set<ComparableIdentifier> tasks,
      OperatorConfigs opConfigs,
      GroupOperatorDescription opDesc)
      throws BindException {
    RootSenderOp rootSenderOp = (RootSenderOp) opDesc;
    ComparableIdentifier senderID = rootSenderOp.sender;
    tasks.add(senderID);
    tasks.addAll(rootSenderOp.receivers);

    Configuration senderConf = getRootConf(rootSenderOp, senderID, rootSenderOp.receivers);
    opConfigs.put(senderID, senderConf);

    for (ComparableIdentifier receiver : rootSenderOp.receivers) {
      Configuration recvConf = getLeafConf(rootSenderOp, senderID, receiver);
      opConfigs.put(receiver, recvConf);
    }
  }

  private static Configuration getLeafConf(
      GroupOperatorDescription opDesc,
      ComparableIdentifier rootID,
      ComparableIdentifier leafID)
      throws BindException {

    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    Configuration leafConf = null;
    switch (opDesc.operatorType) {
      case SCATTER:
        jcb.bindNamedParameter(GroupParameters.Scatter.ReceiverParams.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.Scatter.ReceiverParams.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Scatter.DataCodec.class, opDesc.dataCodecClass);
        leafConf = jcb.build();
        break;

      case BROADCAST:
        jcb.bindNamedParameter(GroupParameters.BroadCast.ReceiverParams.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.BroadCast.ReceiverParams.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.BroadCast.DataCodec.class, opDesc.dataCodecClass);
        leafConf = jcb.build();
        break;

      case GATHER:
        jcb.bindNamedParameter(GroupParameters.Gather.SenderParams.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.Gather.SenderParams.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Gather.DataCodec.class, opDesc.dataCodecClass);
        leafConf = jcb.build();
        break;

      case REDUCE:
        RootReceiverOp reduce = (RootReceiverOp) opDesc;
        jcb.bindNamedParameter(GroupParameters.Reduce.SenderParams.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.Reduce.SenderParams.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Reduce.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.Reduce.ReduceFunction.class, reduce.redFuncClass);
        leafConf = jcb.build();
        break;

      case ALL_GATHER:
        jcb.bindNamedParameter(GroupParameters.AllGather.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.AllGather.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.AllGather.DataCodec.class, opDesc.dataCodecClass);
        leafConf = jcb.build();
        break;

      case ALL_REDUCE:
        SymmetricOpDescription allReduce = (SymmetricOpDescription) opDesc;
        jcb.bindNamedParameter(GroupParameters.AllReduce.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.AllReduce.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.AllReduce.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.AllReduce.ReduceFunction.class, allReduce.redFuncClass);
        leafConf = jcb.build();
        break;

      case REDUCE_SCATTER:
        SymmetricOpDescription reduceScatter = (SymmetricOpDescription) opDesc;
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.SelfId.class, leafID.toString());
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.ParentId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.ReduceFunction.class, reduceScatter.redFuncClass);
        leafConf = jcb.build();
        break;

      default:
        break;
    }

    return leafConf;
  }

  private static Configuration getRootConf(
      GroupOperatorDescription opDesc,
      ComparableIdentifier rootID,
      List<ComparableIdentifier> leaves)
      throws BindException {

    JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
    Configuration rootConf = null;
    switch (opDesc.operatorType) {
      case SCATTER:
        jcb.bindNamedParameter(GroupParameters.Scatter.SenderParams.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Scatter.SenderParams.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.Scatter.DataCodec.class, opDesc.dataCodecClass);
        rootConf = jcb.build();
        break;

      case BROADCAST:
        jcb.bindNamedParameter(GroupParameters.BroadCast.SenderParams.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.BroadCast.SenderParams.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.BroadCast.DataCodec.class, opDesc.dataCodecClass);
        rootConf = jcb.build();
        break;

      case GATHER:
        jcb.bindNamedParameter(GroupParameters.Gather.ReceiverParams.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Gather.ReceiverParams.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.Gather.DataCodec.class, opDesc.dataCodecClass);
        rootConf = jcb.build();
        break;

      case REDUCE:
        RootReceiverOp reduce = (RootReceiverOp) opDesc;
        jcb.bindNamedParameter(GroupParameters.Reduce.ReceiverParams.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.Reduce.ReceiverParams.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.Reduce.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.Reduce.ReduceFunction.class, reduce.redFuncClass);
        rootConf = jcb.build();
        break;

      case ALL_GATHER:
        jcb.bindNamedParameter(GroupParameters.AllGather.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.AllGather.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.AllGather.DataCodec.class, opDesc.dataCodecClass);
        rootConf = jcb.build();
        break;

      case ALL_REDUCE:
        SymmetricOpDescription allReduce = (SymmetricOpDescription) opDesc;
        jcb.bindNamedParameter(GroupParameters.AllReduce.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.AllReduce.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.AllReduce.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.AllReduce.ReduceFunction.class, allReduce.redFuncClass);
        rootConf = jcb.build();
        break;

      case REDUCE_SCATTER:
        SymmetricOpDescription reduceScatter = (SymmetricOpDescription) opDesc;
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.SelfId.class, rootID.toString());
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.ChildIds.class, Utils.listToString(leaves));
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.DataCodec.class, opDesc.dataCodecClass);
        jcb.bindNamedParameter(GroupParameters.ReduceScatter.ReduceFunction.class, reduceScatter.redFuncClass);
        rootConf = jcb.build();
        break;

      default:
        break;
    }

    return rootConf;
  }
}
