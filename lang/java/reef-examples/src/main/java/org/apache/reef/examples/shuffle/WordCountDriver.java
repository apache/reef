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

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.examples.shuffle.params.InputString;
import org.apache.reef.examples.shuffle.params.WordCountTopology;
import org.apache.reef.examples.shuffle.utils.IntegerCodec;
import org.apache.reef.examples.shuffle.utils.StringCodec;
import org.apache.reef.io.network.impl.BindNSClientToTask;
import org.apache.reef.io.network.impl.UnbindNSClientFromTask;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriver;
import org.apache.reef.io.network.shuffle.grouping.impl.AllGrouping;
import org.apache.reef.io.network.shuffle.grouping.impl.KeyGrouping;
import org.apache.reef.io.network.shuffle.topology.ImmutableGroupingDescription;
import org.apache.reef.io.network.shuffle.topology.ImmutableNodePoolDescription;
import org.apache.reef.io.network.shuffle.topology.ImmutableTopologyDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
@Unit
public final class WordCountDriver {
  private static final Logger LOG = Logger.getLogger(WordCountDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final ShuffleDriver shuffleDriver;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;


  private final AtomicInteger allocatedEvalNum;

  private final int mapperNum = 3;
  private final int reducerNum = 2;

  private final String[] inputStringArr;

  private final String[] mapperIds;
  private final String[] reducerIds;

  public static final String MAPPER_ID_PREFIX = "WordCountMapper";
  public static final String REDUCER_ID_PREFIX = "WordCountReducer";
  public static final String AGGREGATOR_ID = "WordCountAggregator";

  public static final String MAPPER_POOL_ID = "mapperPool";
  public static final String REDUCER_POOL_ID = "reducerPool";
  public static final String AGGREGATOR_POOL_ID = "aggregatorPool";

  public static final String SHUFFLE_GROUPING = "shuffleGrouping";
  public static final String AGGREGATING_GROUPING = "aggregatingGrouping";

  private final ConfigurationSerializer confSerializer;
  @Inject
  public WordCountDriver(
      final ConfigurationSerializer confSerializer,
      final EvaluatorRequestor requestor,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer,
      final ShuffleDriver shuffleDriver) {
    LOG.log(Level.FINE, "Instantiated 'WordCountDriver'");
    this.confSerializer = confSerializer;
    this.requestor = requestor;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    this.shuffleDriver = shuffleDriver;

    this.allocatedEvalNum = new AtomicInteger(0);
    this.inputStringArr = new String[mapperNum];
    this.mapperIds = new String[mapperNum];
    this.reducerIds = new String[reducerNum];

    createInputStrings();
    createTaskIds();
    createWordCountTopology();
  }

  private void createInputStrings() {
    final String input = InputString.INPUT.toLowerCase();
    System.out.println(input);
    final int delta = input.length() / mapperNum;
    int index = 0;
    for (int i = 0; i < mapperNum; i++) {
      inputStringArr[i] = input.substring(index, Math.min(input.length(), index + delta));
      index += delta;
    }
  }

  private void createTaskIds() {
    for (int i = 0; i < mapperNum; i++) {
      mapperIds[i] = MAPPER_ID_PREFIX + i;
    }

    for (int i = 0; i < reducerNum; i++) {
      reducerIds[i] = REDUCER_ID_PREFIX + i;
    }
  }

  private void createWordCountTopology() {
    shuffleDriver.submitTopology(
        ImmutableTopologyDescription.newBuilder(WordCountTopology.class)
            .addNodePoolDescription(ImmutableNodePoolDescription.newBuilder(MAPPER_POOL_ID)
                .addNodeIds(mapperIds)
                .build())
            .addNodePoolDescription(ImmutableNodePoolDescription.newBuilder(REDUCER_POOL_ID)
                .addNodeIds(reducerIds)
                .build())
            .addNodePoolDescription(ImmutableNodePoolDescription.newBuilder(AGGREGATOR_POOL_ID)
                .addNodeId(AGGREGATOR_ID)
                .build())
            .addGroupingDescription(ImmutableGroupingDescription.newBuilder(SHUFFLE_GROUPING)
                .setSenderPoolId(MAPPER_POOL_ID)
                .setReceiverPoolId(REDUCER_POOL_ID)
                .setGroupingClass(KeyGrouping.class)
                .setKeyCodecClass(StringCodec.class)
                .setValueCodecClass(IntegerCodec.class)
                .build())
            .addGroupingDescription(ImmutableGroupingDescription.newBuilder(AGGREGATING_GROUPING)
                .setSenderPoolId(REDUCER_POOL_ID)
                .setReceiverPoolId(AGGREGATOR_POOL_ID)
                .setGroupingClass(AllGrouping.class)
                .setKeyCodecClass(StringCodec.class)
                .setValueCodecClass(IntegerCodec.class)
                .build())
            .build()
    );
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      WordCountDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(mapperNum + reducerNum + 1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int allocatedNum = allocatedEvalNum.getAndIncrement();
      final Configuration partialTaskConf;
      final String taskId;
      if (allocatedNum < mapperNum) {
        taskId = mapperIds[allocatedNum];
        partialTaskConf = Tang.Factory.getTang().newConfigurationBuilder(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, MapperTask.class)
            .build())
            .bindNamedParameter(InputString.class, inputStringArr[allocatedNum])
            .build();
      } else if (allocatedNum < mapperNum + reducerNum) {
        taskId = reducerIds[allocatedNum - mapperNum];
        partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, ReducerTask.class)
            .build();
      } else {
        taskId = AGGREGATOR_ID;
        partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, AggregatorTask.class)
            .build();
      }

      final Configuration partialContextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "WordCountContext-" + allocatedNum)
          .build();

      final Configuration netServiceConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
          .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServer.getPort()))
          .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, BindNSClientToTask.class)
          .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, UnbindNSClientFromTask.class)
          .build();

      allocatedEvaluator.submitContextAndServiceAndTask(
          Configurations.merge(shuffleDriver.getContextConfiguration(), partialContextConf),
          netServiceConf,
          Configurations.merge(shuffleDriver.getTaskConfiguration(taskId), partialTaskConf)
      );
    }
  }
}