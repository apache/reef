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
package org.apache.reef.examples.shuffle;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.examples.shuffle.params.WordCountShuffle;
import org.apache.reef.examples.shuffle.utils.IntegerCodec;
import org.apache.reef.examples.shuffle.utils.StringCodec;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.network.impl.BindNetworkConnectionServiceToTask;
import org.apache.reef.io.network.impl.UnbindNetworkConnectionServiceFromTask;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriver;
import org.apache.reef.io.network.shuffle.grouping.impl.AllGroupingStrategy;
import org.apache.reef.io.network.shuffle.grouping.impl.KeyGroupingStrategy;
import org.apache.reef.io.network.shuffle.impl.StaticShuffleManager;
import org.apache.reef.io.network.shuffle.description.GroupingDescriptionImpl;
import org.apache.reef.io.network.shuffle.description.ShuffleDescriptionImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
@Unit
public final class WordCountDriver {
  private static final Logger LOG = Logger.getLogger(WordCountDriver.class.getName());

  private final DataLoadingService dataLoadingService;
  private final ShuffleDriver shuffleDriver;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;


  private final AtomicInteger allocatedMapperNum;
  private final AtomicInteger allocatedReducerNum;

  private final int mapperNum;
  private final int reducerNum;

  private final List<String> mapperIdList;
  private final List<String> reducerIdList;

  public static final String MAPPER_ID_PREFIX = "WordCountMapper";
  public static final String REDUCER_ID_PREFIX = "WordCountReducer";
  public static final String AGGREGATOR_ID = "WordCountAggregator";

  public static final String SHUFFLE_GROUPING = "shuffleGrouping";
  public static final String AGGREGATING_GROUPING = "aggregatingGrouping";

  @Inject
  public WordCountDriver(
      final @Parameter(WordCountREEF.MapperNum.class) int mapperNum,
      final @Parameter(WordCountREEF.ReducerNum.class) int reducerNum,
      final DataLoadingService dataLoadingService,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer,
      final ShuffleDriver shuffleDriver) {
    LOG.log(Level.FINE, "Instantiated 'WordCountDriver'");
    this.mapperNum = mapperNum;
    this.reducerNum = reducerNum;
    this.dataLoadingService = dataLoadingService;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    this.shuffleDriver = shuffleDriver;

    this.allocatedMapperNum = new AtomicInteger(0);
    this.allocatedReducerNum = new AtomicInteger(0);

    this.mapperIdList = new ArrayList<>(mapperNum);
    this.reducerIdList = new ArrayList<>(reducerNum);

    createTaskIds();
    createWordCountTopology();
  }

  private void createTaskIds() {
    for (int i = 0; i < mapperNum; i++) {
      mapperIdList.add(MAPPER_ID_PREFIX + i);
    }

    for (int i = 0; i < reducerNum; i++) {
      reducerIdList.add(REDUCER_ID_PREFIX + i);
    }
  }

  private void createWordCountTopology() {
    final List<String> aggregatorIdList = new ArrayList<>(1);
    aggregatorIdList.add(AGGREGATOR_ID);
    shuffleDriver.registerManager(ShuffleDescriptionImpl.newBuilder(WordCountShuffle.class)
        .addGrouping(
            mapperIdList,
            reducerIdList,
            GroupingDescriptionImpl.newBuilder(SHUFFLE_GROUPING)
                .setGroupingStrategy(KeyGroupingStrategy.class)
                .setKeyCodec(StringCodec.class)
                .setValueCodec(IntegerCodec.class)
                .build())
        .addGrouping(
            reducerIdList,
            aggregatorIdList,
            GroupingDescriptionImpl.newBuilder(AGGREGATING_GROUPING)
                .setGroupingStrategy(AllGroupingStrategy.class)
                .setKeyCodec(StringCodec.class)
                .setValueCodec(IntegerCodec.class)
                .build())
        .build()
        , StaticShuffleManager.class);
  }

  public final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext context) {
      if (dataLoadingService.isDataLoadedContext(context)) {
        final int mapperIndex = allocatedMapperNum.getAndIncrement();
        if (mapperIndex >= mapperNum) {
          throw new RuntimeException("The number of allocated mapper exceeds pre-defined number " + mapperNum);
        }

        context.submitContextAndService(getContextConfiguration(
            mapperIdList.get(mapperIndex) + "-Context"), getServiceConfiguration());
      } else if (dataLoadingService.isComputeContext(context)){
        final int reducerIndex = allocatedReducerNum.getAndIncrement();
        if (reducerIndex < reducerNum) {
          context.submitContextAndService(getContextConfiguration(
              reducerIdList.get(reducerIndex) + "-Context"), getServiceConfiguration());
        } else if (reducerIndex == reducerNum) {
          context.submitContextAndService(getContextConfiguration(
              AGGREGATOR_ID + "-Context"), getServiceConfiguration());
        } else {
          throw new RuntimeException("The number of allocated reducer exceeds pre-defined number " + reducerNum);
        }
      } else if (isMapperContext(context)) {
        submitTask(context, MapperTask.class);
      } else if (isReducerContext(context)) {
        submitTask(context, ReducerTask.class);
      } else if (isAggregatorContext(context)) {
        submitTask(context, AggregatorTask.class);
      }
    }
  }

  private Configuration getContextConfiguration(final String contextId) {
    final Configuration partialContextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, contextId)
        .build();

    return Configurations.merge(shuffleDriver.getContextConfiguration(), partialContextConf);
  }

  private Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServer.getPort()))
        .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, BindNetworkConnectionServiceToTask.class)
        .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, UnbindNetworkConnectionServiceFromTask.class)
        .build();
  }

  private boolean isMapperContext(final ActiveContext context) {
    return context.getId().startsWith(MAPPER_ID_PREFIX);
  }

  private boolean isReducerContext(final ActiveContext context) {
    return context.getId().startsWith(REDUCER_ID_PREFIX);
  }

  private boolean isAggregatorContext(final ActiveContext context) {
    return context.getId().startsWith(AGGREGATOR_ID);
  }

  private void submitTask(final ActiveContext context, final Class<? extends Task> taskClass) {

    final String taskId = getTaskId(context);
    System.out.println("Submit " + taskId + " with " + taskClass);

    final Configuration partialTaskConf = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, taskId)
        .set(TaskConfiguration.TASK, taskClass)
        .build();

    context.submitTask(Configurations.merge(shuffleDriver.getTaskConfiguration(taskId), partialTaskConf));
  }

  private String getTaskId(final ActiveContext context) {
    final int endIndex = context.getId().lastIndexOf("-Context");
    return context.getId().substring(0, endIndex);
  }
}