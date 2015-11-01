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
package org.apache.reef.io.network.group.api.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.GatherOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ScatterOperatorSpec;
import org.apache.reef.io.network.group.impl.driver.CommunicationGroupDriverImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;

/**
 * The driver side interface of a Communication Group
 * Lets one add operators and tasks.
 * Main function is to extract the configuration related
 * to the Group Communication for a task in the comm group
 */
@DriverSide
@DefaultImplementation(CommunicationGroupDriverImpl.class)
public interface CommunicationGroupDriver {

  /**
   * Add the broadcast operator specified by the.
   * 'spec' with name 'operatorName' into this
   * Communication Group
   *
   * @param operatorName
   * @param spec
   * @return
   */
  CommunicationGroupDriver addBroadcast(Class<? extends Name<String>> operatorName, BroadcastOperatorSpec spec);

  /**
   * Add the reduce operator specified by the.
   * 'spec' with name 'operatorName' into this
   * Communication Group
   *
   * @param operatorName
   * @param spec
   * @return
   */
  CommunicationGroupDriver addReduce(Class<? extends Name<String>> operatorName, ReduceOperatorSpec spec);

  /**
   * Add the scatter operator specified by {@code operatorName} and {@code spec}.
   *
   * @param operatorName
   * @param spec
   * @return
   */
  CommunicationGroupDriver addScatter(Class<? extends Name<String>> operatorName, ScatterOperatorSpec spec);

  /**
   * Add the gather operator specified by {@code operatorName} and {@code spec}.
   *
   * @param operatorName
   * @param spec
   * @return
   */
  CommunicationGroupDriver addGather(Class<? extends Name<String>> operatorName, GatherOperatorSpec spec);

  /**
   * This signals to the service that no more.
   * operator specs will be added to this communication
   * group and an attempt to do that will throw an
   * IllegalStateException
   */
  void finalise();

  /**
   * Returns a configuration that includes the partial task
   * configuration passed in as 'taskConf' and makes the
   * current communication group and the operators configured
   * on it available on the Task side. Provides for injection
   * of {@link org.apache.reef.io.network.group.api.task.GroupCommClient}
   *
   * @param taskConf
   * @return
   */
  Configuration getTaskConfiguration(Configuration taskConf);

  /**
   * Add the task represented by this configuration to this
   * communication group. The configuration needs to contain
   * the id of the Task that will be used
   *
   * @param partialTaskConf
   */
  void addTask(Configuration partialTaskConf);
}
