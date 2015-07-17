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
package org.apache.reef.io.network.shuffle.task;

import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescription;
import org.apache.reef.io.network.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.io.network.shuffle.network.ShuffleTupleMessageCodec;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class TupleOperatorFactoryImpl implements TupleOperatorFactory {

  private final String nodeId;
  private final ShuffleTupleMessageCodec globalTupleCodec;
  private final InjectionFuture<ShuffleClient> client;
  private final Injector injector;

  private Map<String, TupleSender> senderMap;
  private Map<String, TupleReceiver> receiverMap;

  @Inject
  public TupleOperatorFactoryImpl(
      @Parameter(TaskConfigurationOptions.Identifier.class) final String nodeId,
      final ShuffleTupleMessageCodec globalTupleCodec,
      final InjectionFuture<ShuffleClient> client,
      final Injector injector) {
    this.nodeId = nodeId;
    this.globalTupleCodec = globalTupleCodec;
    this.client = client;
    this.injector = injector;
    this.senderMap = new ConcurrentHashMap<>();
    this.receiverMap = new ConcurrentHashMap<>();
  }

  private void addTupleCodec(final ShuffleDescription shuffleDescription) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final Configuration tupleCodecConf = confBuilder
        .bindNamedParameter(ShuffleParameters.ShuffleKeyCodec.class, shuffleDescription.getKeyCodecClass())
        .bindNamedParameter(ShuffleParameters.ShuffleValueCodec.class, shuffleDescription.getValueCodecClass())
        .build();
    try {
      final Codec<Tuple> tupleCodec = Tang.Factory.getTang().newInjector(tupleCodecConf).getInstance(TupleCodec.class);
      globalTupleCodec.registerTupleCodec(client.get().getShuffleGroupDescription().getShuffleGroupName(),
          shuffleDescription.getShuffleName(), tupleCodec);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K, V> TupleReceiver<K, V> newTupleReceiver(final ShuffleDescription shuffleDescription) {
    final String shuffleName = shuffleDescription.getShuffleName();

    if (!receiverMap.containsKey(shuffleName)) {
      final ShuffleGroupDescription description = client.get().getShuffleGroupDescription();
      if (!description.getReceiverIdList(shuffleName).contains(nodeId)) {
        throw new RuntimeException(shuffleName + " does not have " + nodeId + " as a receiver.");
      }

      final Configuration receiverConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(ShuffleStrategy.class, shuffleDescription.getShuffleStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(receiverConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);

      try {
        receiverMap.put(shuffleName, forkedInjector.getInstance(TupleReceiver.class));
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An Exception occurred while injecting receiver with " + shuffleDescription, e);
      }
    }

    return receiverMap.get(shuffleName);
  }

  @Override
  public <K, V> TupleSender<K, V> newTupleSender(final ShuffleDescription shuffleDescription) {
    final String shuffleName = shuffleDescription.getShuffleName();

    if (!senderMap.containsKey(shuffleName)) {
      final ShuffleGroupDescription description = client.get().getShuffleGroupDescription();
      if (!description.getSenderIdList(shuffleName).contains(nodeId)) {
        throw new RuntimeException(shuffleName + " does not have " + nodeId + " as a sender.");
      }

      final Configuration senderConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(ShuffleStrategy.class, shuffleDescription.getShuffleStrategyClass())
          .build();

      final Injector forkedInjector = injector.forkInjector(senderConfiguration);
      forkedInjector.bindVolatileInstance(ShuffleClient.class, client.get());
      forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);

      try {
        senderMap.put(shuffleName, forkedInjector.getInstance(TupleSender.class));
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting sender with " +
            shuffleDescription, e);
      }
    }

    return senderMap.get(shuffleName);
  }
}
