/*
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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommServiceDriver;
import org.apache.reef.io.network.group.api.driver.Topology;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessageCodec;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.task.GroupCommNetworkHandlerImpl;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.impl.*;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.SingletonAsserter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sets up various stages to handle REEF events and adds the per communication
 * group stages to them whenever a new communication group is created.
 * <p>
 * Also starts the NameService and the NetworkService on the driver
 */
public final class GroupCommDriverImpl implements GroupCommServiceDriver {
  private static final Logger LOG = Logger.getLogger(GroupCommDriverImpl.class.getName());
  /**
   * TANG instance.
   */
  private static final Tang TANG = Tang.Factory.getTang();

  private final CommunicationGroupDriverFactory commGroupDriverFactory;

  private final AtomicInteger contextIds = new AtomicInteger(0);

  private final IdentifierFactory idFac = new StringIdentifierFactory();

  private final NameServer nameService;

  private final String nameServiceAddr;
  private final int nameServicePort;

  private final Map<Class<? extends Name<String>>, CommunicationGroupDriver> commGroupDrivers = new HashMap<>();

  private final ConfigurationSerializer confSerializer;

  private final NetworkService<GroupCommunicationMessage> netService;

  private final BroadcastingEventHandler<RunningTask> groupCommRunningTaskHandler;
  private final EStage<RunningTask> groupCommRunningTaskStage;
  private final BroadcastingEventHandler<FailedTask> groupCommFailedTaskHandler;
  private final EStage<FailedTask> groupCommFailedTaskStage;
  private final BroadcastingEventHandler<FailedEvaluator> groupCommFailedEvaluatorHandler;
  private final EStage<FailedEvaluator> groupCommFailedEvaluatorStage;
  private final GroupCommMessageHandler groupCommMessageHandler;
  private final EStage<GroupCommunicationMessage> groupCommMessageStage;
  private final int fanOut;

  @Inject
  private GroupCommDriverImpl(final ConfigurationSerializer confSerializer,
                             @Parameter(DriverIdentifier.class) final String driverId,
                             @Parameter(TreeTopologyFanOut.class) final int fanOut,
                             final LocalAddressProvider localAddressProvider,
                             final TransportFactory tpFactory,
                             final NameServer nameService) {
    assert SingletonAsserter.assertSingleton(getClass());
    this.fanOut = fanOut;
    this.nameService = nameService;
    this.nameServiceAddr = localAddressProvider.getLocalAddress();
    this.nameServicePort = nameService.getPort();
    this.confSerializer = confSerializer;
    this.groupCommRunningTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommRunningTaskStage = new SyncStage<>("GroupCommRunningTaskStage", groupCommRunningTaskHandler);
    this.groupCommFailedTaskHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedTaskStage = new SyncStage<>("GroupCommFailedTaskStage", groupCommFailedTaskHandler);
    this.groupCommFailedEvaluatorHandler = new BroadcastingEventHandler<>();
    this.groupCommFailedEvaluatorStage = new SyncStage<>("GroupCommFailedEvaluatorStage",
        groupCommFailedEvaluatorHandler);
    this.groupCommMessageHandler = new GroupCommMessageHandler();
    this.groupCommMessageStage = new SyncStage<>("GroupCommMessageStage", groupCommMessageHandler);

    final Configuration nameResolverConf = Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, nameServiceAddr)
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServicePort)
        .build())
        .build();

    final NameResolver nameResolver;
    try {
      nameResolver = Tang.Factory.getTang().newInjector(nameResolverConf).getInstance(NameResolver.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Failed to instantiate NameResolver", e);
    }

    try {
      final Injector injector = TANG.newInjector();
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class, idFac);
      injector.bindVolatileInstance(NameResolver.class, nameResolver);
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceCodec.class,
          new GroupCommunicationMessageCodec());
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceTransportFactory.class, tpFactory);
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceHandler.class,
          new EventHandler<Message<GroupCommunicationMessage>>() {
            @Override
            public void onNext(final Message<GroupCommunicationMessage> msg) {
              groupCommMessageStage.onNext(Utils.getGCM(msg));
            }
          });
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class,
          new LoggingEventHandler<Exception>());
      this.netService = injector.getInstance(NetworkService.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Failed to instantiate NetworkService", e);
    }
    this.netService.registerId(idFac.getNewInstance(driverId));
    final EStage<GroupCommunicationMessage> senderStage
        = new ThreadPoolStage<>("SrcCtrlMsgSender", new CtrlMsgSender(idFac, netService), 5);

    final Injector injector = TANG.newInjector();
    injector.bindVolatileParameter(GroupCommSenderStage.class, senderStage);
    injector.bindVolatileParameter(DriverIdentifier.class, driverId);
    injector.bindVolatileParameter(GroupCommRunningTaskHandler.class, groupCommRunningTaskHandler);
    injector.bindVolatileParameter(GroupCommFailedTaskHandler.class, groupCommFailedTaskHandler);
    injector.bindVolatileParameter(GroupCommFailedEvalHandler.class, groupCommFailedEvaluatorHandler);
    injector.bindVolatileInstance(GroupCommMessageHandler.class, groupCommMessageHandler);

    try {
      commGroupDriverFactory = injector.getInstance(CommunicationGroupDriverFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CommunicationGroupDriver newCommunicationGroup(final Class<? extends Name<String>> groupName,
                                                        final int numberOfTasks) {
    return newCommunicationGroup(groupName, TreeTopology.class, numberOfTasks, fanOut);
  }

  @Override
  public CommunicationGroupDriver newCommunicationGroup(final Class<? extends Name<String>> groupName,
                                                        final int numberOfTasks, final int customFanOut) {
    return newCommunicationGroup(groupName, TreeTopology.class, numberOfTasks, customFanOut);
  }

  // TODO[JIRA REEF-391]: Allow different topology implementations for different operations in the same CommGroup.
  @Override
  public CommunicationGroupDriver newCommunicationGroup(final Class<? extends Name<String>> groupName,
                                                        final Class<? extends Topology> topologyClass,
                                                        final int numberOfTasks, final int customFanOut) {
    LOG.entering("GroupCommDriverImpl", "newCommunicationGroup",
        new Object[]{Utils.simpleName(groupName), numberOfTasks});

    final CommunicationGroupDriver commGroupDriver;
    try {
      commGroupDriver
          = commGroupDriverFactory.getNewInstance(groupName, topologyClass, numberOfTasks, customFanOut);
    } catch (final InjectionException e) {
      LOG.log(Level.WARNING, "Cannot inject new CommunicationGroupDriver");
      throw new RuntimeException(e);
    }

    commGroupDrivers.put(groupName, commGroupDriver);
    LOG.exiting("GroupCommDriverImpl", "newCommunicationGroup",
        "Created communication group: " + Utils.simpleName(groupName));
    return commGroupDriver;
  }

  @Override
  public boolean isConfigured(final ActiveContext activeContext) {
    LOG.entering("GroupCommDriverImpl", "isConfigured", activeContext.getId());
    final boolean retVal = activeContext.getId().startsWith("GroupCommunicationContext-");
    LOG.exiting("GroupCommDriverImpl", "isConfigured", retVal);
    return retVal;
  }

  @Override
  public Configuration getContextConfiguration() {
    LOG.entering("GroupCommDriverImpl", "getContextConf");
    final Configuration retVal = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
        "GroupCommunicationContext-" + contextIds.getAndIncrement()).build();
    LOG.exiting("GroupCommDriverImpl", "getContextConf", confSerializer.toString(retVal));
    return retVal;
  }

  @Override
  public Configuration getServiceConfiguration() {
    LOG.entering("GroupCommDriverImpl", "getServiceConf");
    final Configuration serviceConfiguration = ServiceConfiguration.CONF.set(ServiceConfiguration.SERVICES,
        NetworkService.class)
        .set(ServiceConfiguration.SERVICES,
            GroupCommNetworkHandlerImpl.class)
        .set(ServiceConfiguration.ON_CONTEXT_STOP,
            NetworkServiceClosingHandler.class)
        .set(ServiceConfiguration.ON_TASK_STARTED,
            BindNSToTask.class)
        .set(ServiceConfiguration.ON_TASK_STOP,
            UnbindNSFromTask.class).build();
    final Configuration retVal = TANG.newConfigurationBuilder(serviceConfiguration)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceCodec.class,
            GroupCommunicationMessageCodec.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceHandler.class,
            GroupCommNetworkHandlerImpl.class)
        .bindNamedParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class,
            ExceptionHandler.class)
        .bindNamedParameter(NameResolverNameServerAddr.class, nameServiceAddr)
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServicePort))
        .bindNamedParameter(NetworkServiceParameters.NetworkServicePort.class, "0").build();
    LOG.exiting("GroupCommDriverImpl", "getServiceConf", confSerializer.toString(retVal));
    return retVal;
  }

  @Override
  public Configuration getTaskConfiguration(final Configuration partialTaskConf) {
    LOG.entering("GroupCommDriverImpl", "getTaskConfiguration", new Object[]{confSerializer.toString(partialTaskConf)});
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf);
    for (final CommunicationGroupDriver commGroupDriver : commGroupDrivers.values()) {
      final Configuration commGroupConf = commGroupDriver.getTaskConfiguration(partialTaskConf);
      if (commGroupConf != null) {
        jcb.bindSetEntry(SerializedGroupConfigs.class, confSerializer.toString(commGroupConf));
      }
    }
    final Configuration retVal = jcb.build();
    LOG.exiting("GroupCommDriverImpl", "getTaskConfiguration", confSerializer.toString(retVal));
    return retVal;
  }

  /**
   * @return the groupCommRunningTaskStage
   */
  @Override
  public EStage<RunningTask> getGroupCommRunningTaskStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommRunningTaskStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommRunningTaskStage", "Returning GroupCommRunningTaskStage");
    return groupCommRunningTaskStage;
  }

  /**
   * @return the groupCommFailedTaskStage
   */
  @Override
  public EStage<FailedTask> getGroupCommFailedTaskStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommFailedTaskStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommFailedTaskStage", "Returning GroupCommFailedTaskStage");
    return groupCommFailedTaskStage;
  }

  /**
   * @return the groupCommFailedEvaluatorStage
   */
  @Override
  public EStage<FailedEvaluator> getGroupCommFailedEvaluatorStage() {
    LOG.entering("GroupCommDriverImpl", "getGroupCommFailedEvaluatorStage");
    LOG.exiting("GroupCommDriverImpl", "getGroupCommFailedEvaluatorStage", "Returning GroupCommFailedEvaluatorStage");
    return groupCommFailedEvaluatorStage;
  }

}
