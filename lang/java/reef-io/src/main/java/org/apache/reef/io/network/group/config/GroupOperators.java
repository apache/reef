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
package org.apache.reef.io.network.group.config;

import org.apache.reef.io.network.group.impl.config.GroupOperatorDescription;
import org.apache.reef.io.network.group.impl.config.RootReceiverOp;
import org.apache.reef.io.network.group.impl.config.RootSenderOp;
import org.apache.reef.io.network.group.impl.config.SymmetricOpDescription;
import org.apache.reef.io.network.group.impl.operators.basic.config.GroupCommOperators;
import org.apache.reef.io.network.group.operators.Reduce.ReduceFunction;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Builder;
import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Exposes the configuration of Group Communication Operators through a fluent
 * syntax using Builders
 * <p/>
 * Also takes responsibility of creating the {@link Configuration} of each task
 * using the added operators by delegating it to {@link GroupCommOperators}
 */
public class GroupOperators {

  /**
   * Storing all the builders created
   */
  private final List<Builder<? extends GroupOperatorDescription>> builders = new ArrayList<>();

  /**
   * Common configs
   */
  private Class<? extends Codec<?>> dataCodecClass;
  private Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * The per task {@link Configuration}
   */
  private Map<ComparableIdentifier, Configuration> configs;

  /**
   * {@link NetworkService} related configs
   */
  private final String nameServiceAddr;
  private final int nameServicePort;
  private final Map<ComparableIdentifier, Integer> id2port;

  /**
   * Used to seal the state after getConfig is called
   */
  private boolean sealed = false;

  public GroupOperators(final Class<? extends Codec<?>> dataCodecClass,
                        final Class<? extends ReduceFunction<?>> redFuncClass,
                        final String nameServiceAddr, final int nameServicePort,
                        final Map<ComparableIdentifier, Integer> id2port) {
    super();
    this.dataCodecClass = dataCodecClass;
    this.redFuncClass = redFuncClass;
    this.nameServiceAddr = nameServiceAddr;
    this.nameServicePort = nameServicePort;
    this.id2port = id2port;
  }

  /**
   * Set the data codec class to be used with all operators that will be added here.
   * Can be overridden for each operator.
   */
  public void setDataCodecClass(final Class<? extends Codec<?>> dataCodecClass) {
    this.dataCodecClass = dataCodecClass;
  }

  /**
   * Set the reduce function class to be used with all reduce operators that will be added.
   * Can be overridden for each operator.
   */
  public void setRedFuncClass(final Class<? extends ReduceFunction<?>> redFuncClass) {
    this.redFuncClass = redFuncClass;
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.Scatter} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.Scatter} Operator
   */
  public RootSenderOp.Builder addScatter() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createRootSenderBuilder(OP_TYPE.SCATTER);
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.Broadcast} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.Broadcast} Operator
   */
  public RootSenderOp.Builder addBroadCast() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createRootSenderBuilder(OP_TYPE.BROADCAST);
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.Gather} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.Gather} Operator
   */
  public RootReceiverOp.Builder addGather() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createRootReceiverBuilder(OP_TYPE.GATHER);
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.Reduce} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.Reduce} Operator
   */
  public RootReceiverOp.Builder addReduce() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createRootReceiverBuilder(OP_TYPE.REDUCE);
  }

  /**
   * Adds an {@link org.apache.reef.io.network.group.operators.AllGather} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.AllGather} Operator
   */
  public SymmetricOpDescription.Builder addAllGather() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createSymOpBuilder(OP_TYPE.ALL_GATHER);
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.AllReduce} operator
   *
   * @return {@link Builder} to build the {@link org.apache.reef.io.network.group.operators.AllReduce} Operator
   */
  public SymmetricOpDescription.Builder addAllReduce() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createSymOpBuilder(OP_TYPE.ALL_REDUCE);
  }

  /**
   * Adds a {@link org.apache.reef.io.network.group.operators.ReduceScatter} operator
   *
   * @return {@link Builder} to build {@link org.apache.reef.io.network.group.operators.ReduceScatter} operator
   */
  public SymmetricOpDescription.Builder addReduceScatter() {
    if (sealed) {
      throw new IllegalStateException("Can't add more operators after getConfig has been called");
    }
    return createSymOpBuilder(OP_TYPE.REDUCE_SCATTER);
  }

  /**
   * Get the Configuration for task with identifier id
   * for all the operators added till now. Its illegal to add
   * any more operators once getConfig has been called.
   */
  public Configuration getConfig(final Identifier id) {
    sealed = true;
    if (configs == null) {
      final List<GroupOperatorDescription> opDesc = new ArrayList<>();
      for (final Builder<? extends GroupOperatorDescription> builder : builders) {
        opDesc.add(builder.build());
      }
      configs = GroupCommOperators
          .getConfigurations(opDesc, nameServiceAddr,
              nameServicePort, id2port);
    }
    return configs.get(id);
  }

  private RootSenderOp.Builder createRootSenderBuilder(final OP_TYPE opType) {
    final RootSenderOp.Builder builder = new RootSenderOp.Builder();
    builders.add(builder);
    builder.setOpertaorType(opType);
    if (dataCodecClass != null) {
      builder.setDataCodecClass(dataCodecClass);
    }
    return builder;
  }

  private RootReceiverOp.Builder createRootReceiverBuilder(final OP_TYPE opType) {
    final RootReceiverOp.Builder builder = new RootReceiverOp.Builder();
    builders.add(builder);
    builder.setOpertaorType(opType);
    if (dataCodecClass != null) {
      builder.setDataCodecClass(dataCodecClass);
    }
    if (opType == OP_TYPE.REDUCE && redFuncClass != null) {
      builder.setRedFuncClass(redFuncClass);
    }
    return builder;
  }

  private SymmetricOpDescription.Builder createSymOpBuilder(final OP_TYPE opType) {
    final SymmetricOpDescription.Builder builder = new SymmetricOpDescription.Builder();
    builders.add(builder);
    builder.setOpertaorType(opType);
    if (dataCodecClass != null) {
      builder.setDataCodecClass(dataCodecClass);
    }
    if ((opType == OP_TYPE.ALL_REDUCE || opType == OP_TYPE.REDUCE_SCATTER)
        && redFuncClass != null) {
      builder.setRedFuncClass(redFuncClass);
    }
    return builder;
  }
}
