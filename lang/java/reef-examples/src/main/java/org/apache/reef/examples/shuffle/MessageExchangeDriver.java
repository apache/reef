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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.impl.BindNetworkConnectionServiceToTask;
import org.apache.reef.io.network.impl.UnbindNetworkConnectionServiceFromTask;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.shuffle.description.ShuffleDescriptionImpl;
import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescriptionImpl;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriver;
import org.apache.reef.io.network.shuffle.strategy.KeyShuffleStrategy;
import org.apache.reef.io.network.shuffle.impl.BasicShuffleManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@DriverSide
@Unit
public final class MessageExchangeDriver {

  public static final String MESSAGE_EXCHANGE_SHUFFLE_GROUP_NAME = "MESSAGE_EXCHANGE_SHUFFLE_GROUP_NAME";
  public static final String MESSAGE_EXCHANGE_SHUFFLE_NAME = "MESSAGE_EXCHANGE_SHUFFLE_NAME";
  public static final String TASK_PREFIX = "TASK";

  private final AtomicInteger allocatedNum;
  private final List<String> taskIds;
  private final EvaluatorRequestor evaluatorRequestor;
  private final ShuffleDriver shuffleDriver;
  private final LocalAddressProvider localAddressProvider;
  private final NameServer nameServer;

  @Inject
  private MessageExchangeDriver(
      @Parameter(TaskNumber.class) final int taskNumber,
      final EvaluatorRequestor evaluatorRequestor,
      final ShuffleDriver shuffleDriver,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer) {
    this.allocatedNum = new AtomicInteger();
    this.taskIds = new ArrayList<>(taskNumber);
    for (int i = 0; i < taskNumber; i++) {
      taskIds.add(TASK_PREFIX + i);
    }
    this.evaluatorRequestor = evaluatorRequestor;
    this.shuffleDriver = shuffleDriver;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    createShuffleGroup();
  }

  private void createShuffleGroup() {
    shuffleDriver.registerManager(
        ShuffleGroupDescriptionImpl.newBuilder(MESSAGE_EXCHANGE_SHUFFLE_GROUP_NAME)
            .addShuffle(taskIds, taskIds, ShuffleDescriptionImpl.newBuilder(MESSAGE_EXCHANGE_SHUFFLE_NAME)
                .setKeyCodec(IntegerCodec.class)
                .setValueCodec(IntegerCodec.class)
                .setShuffleStrategy(KeyShuffleStrategy.class)
                .build())
        .build(), BasicShuffleManager.class
    );
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(taskIds.size())
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
    }
  }

  private Configuration getContextConfiguration() {
    final Configuration partialContextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "MessageExchangeContext")
        .build();

    return Configurations.merge(partialContextConf, shuffleDriver.getContextConfiguration());
  }

  private Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServer.getPort()))
        .bindSetEntry(TaskConfigurationOptions.StartHandlers.class, BindNetworkConnectionServiceToTask.class)
        .bindSetEntry(TaskConfigurationOptions.StopHandlers.class, UnbindNetworkConnectionServiceFromTask.class)
        .build();
  }

  private Configuration getTaskConfiguration(final String taskId) {
    final Configuration partialTaskConf = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, taskId)
        .set(TaskConfiguration.TASK, MessageExchangeTask.class)
        .build();

    return Configurations.merge(partialTaskConf, shuffleDriver.getTaskConfiguration(taskId));
  }


  public final class AllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String taskId = taskIds.get(allocatedNum.getAndIncrement());
      allocatedEvaluator.submitContextAndServiceAndTask(
          getContextConfiguration(), getServiceConfiguration(), getTaskConfiguration(taskId));
    }
  }

  @NamedParameter(short_name = "task_num", default_value = "5")
  public static final class TaskNumber implements Name<Integer> {
  }
}
