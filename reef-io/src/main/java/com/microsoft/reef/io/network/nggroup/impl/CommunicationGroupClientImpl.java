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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.GroupCommOperator;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.nggroup.api.CommGroupNetworkHandler;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.network.nggroup.api.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 */
public class CommunicationGroupClientImpl implements com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient {
  private static final Logger LOG = Logger.getLogger(CommunicationGroupClientImpl.class.getName());

  private final GroupCommNetworkHandler groupCommNetworkHandler;
  private final Class<? extends Name<String>> groupName;
  private final Map<Class<? extends Name<String>>, GroupCommOperator> operators;
  private final Sender sender;

  private final String taskId;

  private final String driverId;

  private final CommGroupNetworkHandler commGroupNetworkHandler;

  private final AtomicBoolean init = new AtomicBoolean(false);

  @Inject
  public CommunicationGroupClientImpl(
      @Parameter(CommunicationGroupName.class) final String groupName,
      @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
      @Parameter(DriverIdentifier.class) final String driverId,
      final GroupCommNetworkHandler groupCommNetworkHandler,
      @Parameter(SerializedOperConfigs.class) final Set<String> operatorConfigs,
      final ConfigurationSerializer configSerializer,
      final NetworkService<GroupCommMessage> netService
  ) {
    this.taskId = taskId;
    this.driverId = driverId;
    LOG.info(groupName + " has GroupCommHandler-"
        + groupCommNetworkHandler.toString());
    this.groupName = Utils.getClass(groupName);
    this.groupCommNetworkHandler = groupCommNetworkHandler;
    this.sender = new Sender(netService);
    this.operators = new TreeMap<>(new Comparator<Class<? extends Name<String>>>() {

      @Override
      public int compare(final Class<? extends Name<String>> o1,
                         final Class<? extends Name<String>> o2) {
        final String s1 = o1.getSimpleName();
        final String s2 = o2.getSimpleName();
        return s1.compareTo(s2);
      }
    });
    try {
      this.commGroupNetworkHandler = Tang.Factory.getTang().newInjector().getInstance(CommGroupNetworkHandler.class);
      this.groupCommNetworkHandler.register(this.groupName, commGroupNetworkHandler);

      for (final String operatorConfigStr : operatorConfigs) {

        final Configuration operatorConfig = configSerializer.fromString(operatorConfigStr);
        final Injector injector = Tang.Factory.getTang().newInjector(operatorConfig);

        injector.bindVolatileParameter(TaskConfigurationOptions.Identifier.class, taskId);
        injector.bindVolatileParameter(CommunicationGroupName.class, groupName);
        injector.bindVolatileInstance(CommGroupNetworkHandler.class, commGroupNetworkHandler);
        injector.bindVolatileInstance(NetworkService.class, netService);
        injector.bindVolatileInstance(CommunicationGroupClient.class, this);

        final GroupCommOperator operator = injector.getInstance(GroupCommOperator.class);
        final String operName = injector.getNamedInstance(OperatorName.class);
        this.operators.put(Utils.getClass(operName), operator);
        LOG.info(operName + " has CommGroupHandler-" + commGroupNetworkHandler.toString());
      }
    } catch (BindException | IOException e) {
      throw new RuntimeException("Unable to deserialize operator config", e);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to deserialize operator config", e);
    }
  }

  @Override
  public Broadcast.Sender getBroadcastSender(final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Broadcast.Sender)) {
      throw new RuntimeException("Configured operator is not a broadcast sender");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    return (Broadcast.Sender) op;
  }

  @Override
  public Reduce.Receiver getReduceReceiver(final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Reduce.Receiver)) {
      throw new RuntimeException("Configured operator is not a reduce receiver");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    return (Reduce.Receiver) op;
  }

  @Override
  public Broadcast.Receiver getBroadcastReceiver(
      final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Broadcast.Receiver)) {
      throw new RuntimeException("Configured operator is not a broadcast receiver");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    return (Broadcast.Receiver) op;
  }

  @Override
  public Reduce.Sender getReduceSender(
      final Class<? extends Name<String>> operatorName) {
    final GroupCommOperator op = operators.get(operatorName);
    if (!(op instanceof Reduce.Sender)) {
      throw new RuntimeException("Configured operator is not a reduce sender");
    }
    commGroupNetworkHandler.addTopologyElement(operatorName);
    return (Reduce.Sender) op;
  }


  @Override
  public void initialize() {
    if (!init.compareAndSet(false, true)) {
      LOG.info("CommGroup-" + groupName + " has been initialized");
      return;
    }
    LOG.info("CommGroup-" + groupName + " is initializing");
    final CountDownLatch initLatch = new CountDownLatch(1);
    final EStage<GroupCommOperator> initStage = new ThreadPoolStage<>(
        new EventHandler<GroupCommOperator>() {

          @Override
          public void onNext(final GroupCommOperator op) {
            op.initialize();
            initLatch.countDown();
          }
        }, operators.size());
    for (final GroupCommOperator op : operators.values()) {
      initStage.onNext(op);
    }
    try {
      initLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for initialization", e);
    }
  }

  @Override
  public GroupChanges getTopologyChanges() {
    LOG.info("Getting Topology Changes");
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      LOG.info("Sending TopologyChanges msg to driver");
      try {
        sender.send(Utils.bldVersionedGCM(groupName, operName,
            Type.TopologyChanges, taskId, op.getVersion(), driverId, 0, new byte[0]));
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException while sending GetTopologyChanges", e);
      }
    }
    final Codec<GroupChanges> changesCodec = new GroupChangesCodec();
    final Map<Class<? extends Name<String>>, GroupChanges> retVal = new HashMap<>();
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      final byte[] changes = commGroupNetworkHandler.waitForTopologyChanges(operName);
      retVal.put(operName, changesCodec.decode(changes));
    }
    return mergeGroupChanges(retVal);
  }

  /**
   * @param perOpChanges
   * @return
   */
  private GroupChanges mergeGroupChanges(
      final Map<Class<? extends Name<String>>, GroupChanges> perOpChanges) {
    final GroupChanges changes = new GroupChangesImpl(false);
    for (final GroupChanges change : perOpChanges.values()) {
      if (change.exist()) {
        changes.setChanges(true);
        break;
      }
    }
    return changes;
  }

  @Override
  public void updateTopology() {
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      LOG.info("Sending UpdateTopology msg to driver" + driverId);
      try {
        sender.send(Utils.bldVersionedGCM(groupName, operName,
            Type.UpdateTopology, taskId, op.getVersion(), driverId, 0, new byte[0]));
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException while sending UpdateTopology", e);
      }
    }
    for (final GroupCommOperator op : operators.values()) {
      final Class<? extends Name<String>> operName = op.getOperName();
      while (true) {
        final GroupCommMessage msg = commGroupNetworkHandler
            .waitForTopologyUpdate(operName);
        if (!msg.hasVersion()) {
          throw new RuntimeException(getQualifiedName()
              + "can only deal with versioned msgs");
        }
        final int msgVersion = msg.getVersion();
        final GroupCommOperator operator = operators.get(Utils.getClass(msg
            .getOperatorname()));
        final int nodeVersion = operator.getVersion();
        if (msgVersion < nodeVersion) {
          LOG.warning(getQualifiedName() + "Received a ver-" + msgVersion
              + " msg while expecting ver-" + nodeVersion + ". Discarding msg");
          continue;
        }
        break;
      }
    }
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + " ";
  }

  @Override
  public Class<? extends Name<String>> getName() {
    return groupName;
  }

}
