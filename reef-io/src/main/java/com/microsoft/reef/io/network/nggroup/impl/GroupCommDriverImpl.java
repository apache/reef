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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.context.ServiceConfiguration;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.GCMCodec;
import com.microsoft.reef.io.network.impl.*;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedGroupConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.impl.SyncStage;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.NetUtils;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class GroupCommDriverImpl implements GroupCommDriver {
  private static final Logger LOG = Logger.getLogger(GroupCommDriverImpl.class.getName());
  /**
   * TANG instance
   */
  private static final Tang tang = Tang.Factory.getTang();

  private final AtomicInteger contextIds = new AtomicInteger(0);

  private final IdentifierFactory idFac = new StringIdentifierFactory();

  private final NameServer nameService = new NameServer(0, idFac);

  private final String nameServiceAddr;
  private final int nameServicePort;

  private final Map<Class<? extends Name<String>>, CommunicationGroupDriver> commGroupDrivers = new HashMap<>();

  private final ConfigurationSerializer confSerializer;

  private final NetworkService<GroupCommMessage> netService;

  private final EStage<GroupCommMessage> senderStage;


  private final String driverId;
  private final BroadcastingEventHandler<RunningTask> groupCommRunningTaskHandler;
  private final EStage<RunningTask> groupCommRunningTaskStage;
  private final BroadcastingEventHandler<FailedTask> groupCommFailedTaskHandler;
  private final EStage<FailedTask> groupCommFailedTaskStage;
  private final BroadcastingEventHandler<FailedEvaluator> groupCommFailedEvaluatorHandler;
  private final EStage<FailedEvaluator> groupCommFailedEvaluatorStage;
  private final GroupCommMessageHandler groupCommMessageHandler;
  private final EStage<GroupCommMessage> groupCommMessageStage;

  @Inject
  public GroupCommDriverImpl(final ConfigurationSerializer confSerializer,
                             @Parameter(DriverIdentifier.class) final String driverId) {
    this.driverId = driverId;
    this.nameServiceAddr = NetUtils.getLocalAddress();
    this.nameServicePort = nameService.getPort();
    this.confSerializer = confSerializer;
    this.groupCommRunningTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommRunningTaskStage = new SyncStage<>("GroupCommRunningTaskStage", groupCommRunningTaskHandler);
    this.groupCommFailedTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedTaskStage = new SyncStage<>("GroupCommFailedTaskStage", groupCommFailedTaskHandler);
    this.groupCommFailedEvaluatorHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedEvaluatorStage = new SyncStage<>("GroupCommFailedEvaluatorStage", groupCommFailedEvaluatorHandler);
    this.groupCommMessageHandler = new GroupCommMessageHandler();
    this.groupCommMessageStage = new SingleThreadStage<>("GroupCommMessageStage", groupCommMessageHandler, 100 * 1000);
    this.netService = new NetworkService<>(idFac, 0, nameServiceAddr,
        nameServicePort, new GCMCodec(),
        new MessagingTransportFactory(),
        new EventHandler<Message<GroupCommMessage>>() {

          @Override
          public void onNext(final Message<GroupCommMessage> msg) {
            final Iterator<GroupCommMessage> gcmIterator = msg.getData().iterator();
            if (gcmIterator.hasNext()) {
              final GroupCommMessage gcm = gcmIterator.next();
              if (gcmIterator.hasNext()) {
                throw new RuntimeException("Expecting exactly one GCM object inside Message but found more");
              }
              /*final Class<? extends Name<String>> groupName = Utils.getClass(gcm.getGroupname());
              commGroupDrivers.get(groupName).handle(gcm);*/
              groupCommMessageStage.onNext(gcm);
            } else {
              throw new RuntimeException("Expecting exactly one GCM object inside Message but found none");
            }
          }
        },
        new LoggingEventHandler<Exception>());
    this.netService.registerId(idFac.getNewInstance(driverId));
    this.senderStage = new ThreadPoolStage<>(
        "SrcCtrlMsgSender", new EventHandler<GroupCommMessage>() {
      @Override
      public void onNext(final GroupCommMessage srcCtrlMsg) {

        final Identifier id = GroupCommDriverImpl.this.idFac.getNewInstance(srcCtrlMsg.getDestid());

        final Connection<GroupCommMessage> link = GroupCommDriverImpl.this.netService.newConnection(id);
        try {
          LOG.info("Sending source ctrl msg " + srcCtrlMsg.getType() + " for " + srcCtrlMsg.getSrcid() + " to " + id);
//          LOG.info("Opening connection to " + id);
          link.open();
//          LOG.info("Writing data to " + id);
          link.write(srcCtrlMsg);
        } catch (final NetworkException e) {
          LOG.log(Level.WARNING, "Unable to send ctrl task msg to parent " + id, e);
          throw new RuntimeException("Unable to send ctrl task msg to parent " + id, e);
        }
      }
    }, 5);

  }

  @Override
  public CommunicationGroupDriver newCommunicationGroup(
      final Class<? extends Name<String>> groupName, final int numberOfTasks) {
    final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler = new BroadcastingEventHandler<>();
    final BroadcastingEventHandler<GroupCommMessage> commGroupMessageHandler = new BroadcastingEventHandler<>();
    final CommunicationGroupDriver commGroupDriver = new
        CommunicationGroupDriverImpl(
        groupName,
        confSerializer,
        senderStage,
        commGroupRunningTaskHandler,
        commGroupFailedTaskHandler,
        commGroupFailedEvaluatorHandler,
        commGroupMessageHandler,
        driverId,
        numberOfTasks);
    commGroupDrivers.put(groupName, commGroupDriver);
    groupCommRunningTaskHandler.addHandler(commGroupRunningTaskHandler);
    groupCommFailedTaskHandler.addHandler(commGroupFailedTaskHandler);
    groupCommMessageHandler.addHandler(groupName, commGroupMessageHandler);
    return commGroupDriver;
  }

  @Override
  public boolean configured(final ActiveContext activeContext) {
    return activeContext.getId().startsWith("GroupCommunicationContext-");
  }

  @Override
  public Configuration getContextConf() {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "GroupCommunicationContext-" + contextIds.getAndIncrement())
        .build();
  }

  @Override
  public Configuration getServiceConf() {
    final Configuration serviceConfiguration = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, NetworkService.class)
        .set(ServiceConfiguration.SERVICES, GroupCommNetworkHandlerImpl.class)
        .set(ServiceConfiguration.ON_CONTEXT_STOP, NetworkServiceClosingHandler.class)
        .set(ServiceConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSFromTask.class)
        .build();
    return tang.newConfigurationBuilder(serviceConfiguration)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class, GCMCodec.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class, GroupCommNetworkHandlerImpl.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
        .bindNamedParameter(NameServerParameters.NameServerAddr.class, nameServiceAddr)
        .bindNamedParameter(NameServerParameters.NameServerPort.class, Integer.toString(nameServicePort))
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, "0")
        .build();
  }

  @Override
  public Configuration getTaskConfiguration(final Configuration partialTaskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf);
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers.values()) {
      final Configuration commGroupConf = commGroupDriver.getConfiguration(partialTaskConf);
      jcb.bindSetEntry(SerializedGroupConfigs.class, confSerializer.toString(commGroupConf));
    }
    return jcb.build();
  }

  /**
   * @return the groupCommRunningTaskStage
   */
  @Override
  public EStage<RunningTask> getGroupCommRunningTaskStage() {
    return groupCommRunningTaskStage;
  }

  /**
   * @return the groupCommFailedTaskStage
   */
  @Override
  public EStage<FailedTask> getGroupCommFailedTaskStage() {
    return groupCommFailedTaskStage;
  }

  /**
   * @return the groupCommFailedEvaluatorStage
   */
  @Override
  public EStage<FailedEvaluator> getGroupCommFailedEvaluatorStage() {
    return groupCommFailedEvaluatorStage;
  }

}
